package ants

import (
	"log"
	"runtime"
	"time"
)

// goWorker是执行任务的实际执行者，
//它启动一个接受任务的goroutine并
//执行函数调用。
type goWorker struct {
	//拥有此工作人员的池。
	pool *Pool

	//任务是一项应完成的工作。
	task chan func()

	//将工作者重新放入队列时，recycleTime将被更新
	recycleTime time.Time
}

//运行启动goroutine以重复该过程
//执行函数调用。
func (w *goWorker) run() {
	w.pool.incRunning()
	go func() {
		defer func() {
			w.pool.decRunning()
			if p := recover(); p != nil {
				if w.pool.panicHandler != nil {
					w.pool.panicHandler(p)
				} else {
					log.Printf("worker exits from a panic: %v\n", p)
					var buf [4096]byte
					n := runtime.Stack(buf[:], false)
					log.Printf("worker exits from panic: %s\n", string(buf[:n]))
				}
			}
			w.pool.workerCache.Put(w)
		}()

		for f := range w.task {
			if f == nil {
				return
			}
			f()
			if ok := w.pool.revertWorker(w); !ok {
				return
			}
		}
	}()
}
