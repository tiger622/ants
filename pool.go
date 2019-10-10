package ants

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/panjf2000/ants/v2/internal"
)

// Pool接受来自客户端的任务，它通过回收goroutine将goroutine的总数限制为给定数目。
type Pool struct {
	// 池的容量
	capacity int32

	// running是当前正在运行的goroutine的数量
	running int32

	// expiryDuration设置每个工作人员的过期时间（秒）
	expiryDuration time.Duration

	// 工人是存储可用工人的切片。
	workers []*goWorker

	// release用于通知池自身已关闭
	release int32

	// 锁定以进行同步操作
	lock sync.Locker

	// 继续等待获取空闲工人
	cond *sync.Cond

	// 一旦确保释放此池将只执行一次。
	once sync.Once

	// workerCache在function：retrieveWorker中加快了可用工人的获取
	workerCache sync.Pool

	// panicHandler用于处理每个工作程序goroutine中的异常。
	//如果为零，异常将再次从工作人员goroutine中抛出。
	panicHandler func(interface{})

	//在pool.Submit上阻止goroutine的最大数量。
	// 0（默认值）表示没有此限制。
	maxBlockingTasks int32

	// goroutine已在pool.Submit上被阻止
	//受pool.lock保护
	blockingNum int32

	//当nonblocking为true时，Pool.Submit将永远不会被阻塞。
	//当无法一次完成Pool.Submit时，将返回ErrPoolOverload。
	//当非阻塞为true时，MaxBlockingTasks不起作用。
	nonblocking bool
}

//定期清除过期的工作人员。
func (p *Pool) periodicallyPurge() {
	heartbeat := time.NewTicker(p.expiryDuration)
	defer heartbeat.Stop()

	var expiredWorkers []*goWorker
	for range heartbeat.C {
		if atomic.LoadInt32(&p.release) == CLOSED {
			break
		}
		currentTime := time.Now()
		p.lock.Lock()
		idleWorkers := p.workers
		n := len(idleWorkers)
		var i int
		for i = 0; i < n && currentTime.Sub(idleWorkers[i].recycleTime) > p.expiryDuration; i++ {
		}
		expiredWorkers = append(expiredWorkers[:0], idleWorkers[:i]...)
		if i > 0 {
			m := copy(idleWorkers, idleWorkers[i:])
			for i = m; i < n; i++ {
				idleWorkers[i] = nil
			}
			p.workers = idleWorkers[:m]
		}
		p.lock.Unlock()

		//通知过时的工人停止。
		//此通知必须在p.lock之外，因为w.task
		//如果有很多工人，可能会阻塞并且可能会花费大量时间
		//位于非本地CPU上。
		for i, w := range expiredWorkers {
			w.task <- nil
			expiredWorkers[i] = nil
		}

		//可能会清理所有工作程序（没有任何工作程序在运行）
		//虽然某些调用程序仍然卡在“ p.cond.Wait（）”中，
		//然后应该唤醒所有这些调用者。
		if p.Running() == 0 {
			p.cond.Broadcast()
		}
	}
}

// NewPool生成一个蚂蚁池实例。
func NewPool(size int, options ...Option) (*Pool, error) {
	if size <= 0 {
		return nil, ErrInvalidPoolSize
	}

	opts := new(Options)
	for _, option := range options {
		option(opts)
	}

	if expiry := opts.ExpiryDuration; expiry < 0 {
		return nil, ErrInvalidPoolExpiry
	} else if expiry == 0 {
		opts.ExpiryDuration = DefaultCleanIntervalTime
	}

	p := &Pool{
		capacity:         int32(size),
		expiryDuration:   opts.ExpiryDuration,
		nonblocking:      opts.Nonblocking,
		maxBlockingTasks: int32(opts.MaxBlockingTasks),
		panicHandler:     opts.PanicHandler,
		lock:             internal.NewSpinLock(),
	}
	p.workerCache = sync.Pool{
		New: func() interface{} {
			return &goWorker{
				pool: p,
				task: make(chan func(), workerChanCap),
			}
		},
	}
	if opts.PreAlloc {
		p.workers = make([]*goWorker, 0, size)
	}
	p.cond = sync.NewCond(p.lock)

	//启动goroutine定期清理过期的工作程序。
	go p.periodicallyPurge()

	return p, nil
}

//---------------------------------------------------------------------------

//提交将任务提交到该池。
func (p *Pool) Submit(task func()) error {
	if atomic.LoadInt32(&p.release) == CLOSED {
		return ErrPoolClosed
	}
	var w *goWorker
	if w = p.retrieveWorker(); w == nil {
		return ErrPoolOverload
	}
	w.task <- task
	return nil
}

// Running返回当前正在运行的goroutine的数量。
func (p *Pool) Running() int {
	return int(atomic.LoadInt32(&p.running))
}

// Free返回可用的goroutines工作。
func (p *Pool) Free() int {
	return p.Cap() - p.Running()
}

// Cap返回该池的容量。
func (p *Pool) Cap() int {
	return int(atomic.LoadInt32(&p.capacity))
}

// Tune更改此池的容量。
func (p *Pool) Tune(size int) {
	if p.Cap() == size {
		return
	}
	atomic.StoreInt32(&p.capacity, int32(size))
}

// Release关闭此池。
func (p *Pool) Release() {
	p.once.Do(func() {
		atomic.StoreInt32(&p.release, 1)
		p.lock.Lock()
		idleWorkers := p.workers
		for i, w := range idleWorkers {
			w.task <- nil
			idleWorkers[i] = nil
		}
		p.workers = nil
		p.lock.Unlock()
	})
}

//---------------------------------------------------------------------------

// incRunning增加当前正在运行的goroutine的数量。
func (p *Pool) incRunning() {
	atomic.AddInt32(&p.running, 1)
}

// decRunning减少当前正在运行的goroutine的数量。
func (p *Pool) decRunning() {
	atomic.AddInt32(&p.running, -1)
}

// refreshWorker返回一个可用的工作程序来运行任务。
func (p *Pool) retrieveWorker() *goWorker {
	var w *goWorker
	spawnWorker := func() {
		w = p.workerCache.Get().(*goWorker)
		w.run()
	}

	p.lock.Lock()
	idleWorkers := p.workers
	n := len(idleWorkers) - 1
	if n >= 0 {
		w = idleWorkers[n]
		idleWorkers[n] = nil
		p.workers = idleWorkers[:n]
		p.lock.Unlock()
	} else if p.Running() < p.Cap() {
		p.lock.Unlock()
		spawnWorker()
	} else {
		if p.nonblocking {
			p.lock.Unlock()
			return nil
		}
	Reentry:
		if p.maxBlockingTasks != 0 && p.blockingNum >= p.maxBlockingTasks {
			p.lock.Unlock()
			return nil
		}
		p.blockingNum++
		p.cond.Wait()
		p.blockingNum--
		if p.Running() == 0 {
			p.lock.Unlock()
			spawnWorker()
			return w
		}
		l := len(p.workers) - 1
		if l < 0 {
			goto Reentry
		}
		w = p.workers[l]
		p.workers[l] = nil
		p.workers = p.workers[:l]
		p.lock.Unlock()
	}
	return w
}

// revertWorker将工作程序放回空闲池，从而回收goroutine。
func (p *Pool) revertWorker(worker *goWorker) bool {
	if atomic.LoadInt32(&p.release) == CLOSED || p.Running() > p.Cap() {
		return false
	}
	worker.recycleTime = time.Now()
	p.lock.Lock()
	p.workers = append(p.workers, worker)

	//通知停留在“ retrieveWorker（）”中的调用者该工作队列中有可用的工作程序。
	p.cond.Signal()
	p.lock.Unlock()
	return true
}
