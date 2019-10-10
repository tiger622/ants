// MIT License

// Copyright (c) 2018 Andy Pan

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package ants

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/panjf2000/ants/v2/internal"
)

// PoolWithFunc接受来自客户端的任务，它通过回收goroutine将goroutine的总数限制为给定数目。
type PoolWithFunc struct {
	//池的容量。
	capacity int32

	// running是当前正在运行的goroutine的数量。
	running int32

	// expiryDuration设置每个工作人员的过期时间（秒）。
	expiryDuration time.Duration

	//工人是存储可用工人的切片。
	workers []*goWorkerWithFunc

	// release用于通知池自身已关闭。
	release int32

	//锁定以进行同步操作。
	lock sync.Locker

	//继续等待获取空闲工人。
	cond *sync.Cond

	// poolFunc是用于处理任务的函数。
	poolFunc func(interface{})

	//一旦确保释放此池将只执行一次。
	once sync.Once

	// workerCache在function：retrieveWorker中加快了可用工人的获取。
	workerCache sync.Pool

	// panicHandler用于处理每个工作程序goroutine中的恐慌。
	//如果为零，恐慌将再次从工作人员goroutine中抛出。
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
func (p *PoolWithFunc) periodicallyPurge() {
	heartbeat := time.NewTicker(p.expiryDuration)
	defer heartbeat.Stop()

	var expiredWorkers []*goWorkerWithFunc
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
			w.args <- nil
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

// NewPoolWithFunc生成具有特定功能的蚂蚁池实例。
func NewPoolWithFunc(size int, pf func(interface{}), options ...Option) (*PoolWithFunc, error) {
	if size <= 0 {
		return nil, ErrInvalidPoolSize
	}

	if pf == nil {
		return nil, ErrLackPoolFunc
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

	p := &PoolWithFunc{
		capacity:         int32(size),
		expiryDuration:   opts.ExpiryDuration,
		poolFunc:         pf,
		nonblocking:      opts.Nonblocking,
		maxBlockingTasks: int32(opts.MaxBlockingTasks),
		panicHandler:     opts.PanicHandler,
		lock:             internal.NewSpinLock(),
	}
	p.workerCache = sync.Pool{
		New: func() interface{} {
			return &goWorkerWithFunc{
				pool: p,
				args: make(chan interface{}, workerChanCap),
			}
		},
	}
	if opts.PreAlloc {
		p.workers = make([]*goWorkerWithFunc, 0, size)
	}
	p.cond = sync.NewCond(p.lock)

	//启动goroutine定期清理过期的工作程序。
	go p.periodicallyPurge()

	return p, nil
}

//---------------------------------------------------------------------------

// Invoke将任务提交到池中。
func (p *PoolWithFunc) Invoke(args interface{}) error {
	if atomic.LoadInt32(&p.release) == CLOSED {
		return ErrPoolClosed
	}
	var w *goWorkerWithFunc
	if w = p.retrieveWorker(); w == nil {
		return ErrPoolOverload
	}
	w.args <- args
	return nil
}

// Running返回当前正在运行的goroutine的数量。
func (p *PoolWithFunc) Running() int {
	return int(atomic.LoadInt32(&p.running))
}

// Free返回可用的goroutines。
func (p *PoolWithFunc) Free() int {
	return p.Cap() - p.Running()
}

// Cap返回该池的容量。
func (p *PoolWithFunc) Cap() int {
	return int(atomic.LoadInt32(&p.capacity))
}

//调整此池的容量。
func (p *PoolWithFunc) Tune(size int) {
	if p.Cap() == size {
		return
	}
	atomic.StoreInt32(&p.capacity, int32(size))
}

//发布关闭此池。
func (p *PoolWithFunc) Release() {
	p.once.Do(func() {
		atomic.StoreInt32(&p.release, 1)
		p.lock.Lock()
		idleWorkers := p.workers
		for i, w := range idleWorkers {
			w.args <- nil
			idleWorkers[i] = nil
		}
		p.workers = nil
		p.lock.Unlock()
	})
}

//---------------------------------------------------------------------------

// incRunning增加当前正在运行的goroutine的数量。
func (p *PoolWithFunc) incRunning() {
	atomic.AddInt32(&p.running, 1)
}

// decRunning减少当前正在运行的goroutine的数量。
func (p *PoolWithFunc) decRunning() {
	atomic.AddInt32(&p.running, -1)
}

// refreshWorker返回一个可用的工作程序来运行任务。
func (p *PoolWithFunc) retrieveWorker() *goWorkerWithFunc {
	var w *goWorkerWithFunc
	spawnWorker := func() {
		w = p.workerCache.Get().(*goWorkerWithFunc)
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
func (p *PoolWithFunc) revertWorker(worker *goWorkerWithFunc) bool {
	if atomic.LoadInt32(&p.release) == CLOSED || p.Running() > p.Cap() {
		return false
	}
	worker.recycleTime = time.Now()
	p.lock.Lock()
	p.workers = append(p.workers, worker)

	// Notify the invoker stuck in 'retrieveWorker()' of there is an available worker in the worker queue.
	p.cond.Signal()
	p.lock.Unlock()
	return true
}
