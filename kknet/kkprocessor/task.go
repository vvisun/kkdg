package kkprocessor

import (
	"errors"
	"io"
	"net"
	"sync"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

type RecvMessage struct {
	connId kknet.CONN_ID
	packet *kkbuffer.ByteBuffer
}

//-------------------------------- task --------------------------------

type (
	// 任务队列
	// Task queue
	WorkerQueue struct {
		// mu 互斥锁
		// mutex
		mu sync.Mutex

		// q 双端队列，用于存储异步任务
		// double-ended queue to store asynchronous jobs
		q Deque[asyncJob]

		// maxConcurrency 最大并发数
		// maximum concurrency
		maxConcurrency int32

		// curConcurrency 当前并发数
		// current concurrency
		curConcurrency int32
	}

	// 异步任务
	// Asynchronous job
	asyncJob func()
)

// 创建一个任务队列
// Creates a task queue
// @param maxConcurrency 最大并发数，1表示串行，大于1表示并发
func NewWorkerQueue(maxConcurrency int32) *WorkerQueue {
	c := &WorkerQueue{
		mu:             sync.Mutex{},
		maxConcurrency: maxConcurrency,
		curConcurrency: 0,
	}
	return c
}

// 获取一个任务
// Retrieves a job from the worker queue
func (c *WorkerQueue) getJob(newJob asyncJob, delta int32) asyncJob {
	c.mu.Lock()
	defer c.mu.Unlock()

	if newJob != nil {
		c.q.PushBack(newJob)
	}
	c.curConcurrency += delta
	if c.curConcurrency >= c.maxConcurrency {
		return nil
	}
	var job = c.q.PopFront()
	if job == nil {
		return nil
	}
	c.curConcurrency++
	return job
}

// 循环执行任务
// Do continuously executes jobs in the worker queue
func (c *WorkerQueue) do(job asyncJob) {
	for job != nil {
		job()
		job = c.getJob(nil, -1)
	}
}

// Push 追加任务, 有资源空闲的话会立即执行
// Adds a job to the queue and executes it immediately if resources are available
func (c *WorkerQueue) Push(job asyncJob) {
	if nextJob := c.getJob(job, 0); nextJob != nil {
		go c.do(nextJob)
	}
}

//-------------------------------- channel --------------------------------

type channel chan struct{}

func (c channel) add() { c <- struct{}{} }

func (c channel) done() { <-c }

func (c channel) Go(m *RecvMessage, f func(*RecvMessage) error) error {
	c.add()
	go func() {
		_ = f(m)
		c.done()
	}()
	return nil
}

//-------------------------------- write processor utils --------------------------------

// defaultIsWriteFnRetryable 默认判断：连接已关闭等致命错误不重试。
func defaultIsWriteFnRetryable(err error) bool {
	if err == nil {
		return true
	}
	if errors.Is(err, kkerrors.ErrConnectionClosed) ||
		errors.Is(err, kkerrors.ErrInvalidPacket) ||
		errors.Is(err, kkerrors.ErrSendQueueFull) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, io.ErrClosedPipe) {
		return false
	}
	// 其他错误（如临时 EAGAIN、超时等）允许重试
	return true
}
