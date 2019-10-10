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
	"errors"
	"math"
	"runtime"
	"time"
)

const (
	// DefaultAntsPoolSize是默认goroutine池的默认容量
	DefaultAntsPoolSize = math.MaxInt32

	// DefaultCleanIntervalTime是清理goroutine的间隔时间。
	DefaultCleanIntervalTime = time.Second

	// CLOSED表示该池已关闭。
	CLOSED = 1
)

var (
	// Ants API的错误类型。
	// ------------------------------------------------ ---------------------------

	//将负数设置为池容量时，将返回ErrInvalidPoolSize。
	ErrInvalidPoolSize = errors.New("invalid size for pool")

	//当调用者不提供池功能时，将返回ErrLackPoolFunc。
	ErrLackPoolFunc = errors.New("must provide function for pool")

	//将负数设置为清除goroutine的定期持续时间时，将返回ErrInvalidPoolExpiry。
	ErrInvalidPoolExpiry = errors.New("invalid expiry for pool")

	//将任务提交到封闭池时，将返回ErrPoolClosed。
	ErrPoolClosed = errors.New("this pool has been closed")

	//当池已满且没有可用的工作程序时，将返回ErrPoolOverload。
	ErrPoolOverload = errors.New("too many goroutines blocked on submit or Nonblocking is set")
	//---------------------------------------------------------------------------

	// workerChanCap确定worker的通道是否应为缓冲通道
	//以获得最佳性能。受到fasthttp的启发，网址为https://github.com/valyala/fasthttp/blob/master/workerpool.go#L139
	workerChanCap = func() int {
		//如果GOMAXPROCS = 1，则使用阻塞workerChan。
		//这会立即将Serve切换到WorkerFunc，结果
		//性能更高（至少在go1.5以下）。
		if runtime.GOMAXPROCS(0) == 1 {
			return 0
		}

		//如果GOMAXPROCS> 1，请使用非阻塞workerChan，
		//因为否则，服务调用方（接受方）可能会滞后于接受
		//如果WorkerFunc受CPU限制，则为新连接。
		return 1
	}()

	//导入蚂蚁时初始化实例池。
	defaultAntsPool, _ = NewPool(DefaultAntsPoolSize)
)

// Option表示可选功能。
type Option func(opts *Options)

//选项包含实例化蚂蚁池时将应用的所有选项。
type Options struct {
	// ExpiryDuration设置每个工作人员的过期时间（秒）。
	ExpiryDuration time.Duration

	// PreAlloc指示在初始化Pool时是否对内存进行预分配。
	PreAlloc bool

	//在pool.Submit上阻止goroutine的最大数量。
	// 0（默认值）表示没有此限制。
	MaxBlockingTasks int

	//当Nonblocking为true时，Pool.Submit将永远不会被阻塞。
	//当无法一次完成Pool.Submit时，将返回ErrPoolOverload。
	//当Nonblocking为true时，MaxBlockingTasks不起作用。
	Nonblocking bool

	// PanicHandler用于处理每个工作程序goroutine中的恐慌。
	//如果为零，恐慌将再次从工作人员goroutine中抛出。
	PanicHandler func(interface{})
}

// WithOptions接受整个选项配置。
func WithOptions(options Options) Option {
	return func(opts *Options) {
		*opts = options
	}
}

// WithExpiryDuration设置清理goroutine的间隔时间。
func WithExpiryDuration(expiryDuration time.Duration) Option {
	return func(opts *Options) {
		opts.ExpiryDuration = expiryDuration
	}
}

// WithPreAlloc指示是否应为工作者malloc。
func WithPreAlloc(preAlloc bool) Option {
	return func(opts *Options) {
		opts.PreAlloc = preAlloc
	}
}

// WithMaxBlockingTasks设置在达到池的容量时被阻止的goroutine的最大数量。
func WithMaxBlockingTasks(maxBlockingTasks int) Option {
	return func(opts *Options) {
		opts.MaxBlockingTasks = maxBlockingTasks
	}
}

// WithNonblocking表示在没有可用工作程序时，池将返回nil。
func WithNonblocking(nonblocking bool) Option {
	return func(opts *Options) {
		opts.Nonblocking = nonblocking
	}
}

// WithPanicHandler设置紧急处理程序。
func WithPanicHandler(panicHandler func(interface{})) Option {
	return func(opts *Options) {
		opts.PanicHandler = panicHandler
	}
}

// Submit将任务提交到池中。
func Submit(task func()) error {
	return defaultAntsPool.Submit(task)
}

// Running返回当前正在运行的goroutine的数量。
func Running() int {
	return defaultAntsPool.Running()
}

// Cap返回此默认池的容量。
func Cap() int {
	return defaultAntsPool.Cap()
}

// Free返回可用的goroutines工作。
func Free() int {
	return defaultAntsPool.Free()
}

// Release关闭默认池。
func Release() {
	defaultAntsPool.Release()
}
