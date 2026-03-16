package taskqueue

import (
	"sync"

	"github.com/vvisun/kkdg/utils/queues/dqueue"
)

type (
	// 任务队列
	// Task queue
	WorkerQueue struct {
		// mu 互斥锁
		// mutex
		mu sync.Mutex

		// q 双端队列，用于存储异步任务
		// double-ended queue to store asynchronous jobs
		q dqueue.Deque[asyncJob]

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
	if maxConcurrency < 1 {
		maxConcurrency = 1
	}
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

// 获取当前队列中待执行的任务数量
// Retrieves the number of jobs in the queue that are waiting to be executed
func (c *WorkerQueue) Len() int {
	c.mu.Lock()
	cnt := c.q.Len()
	c.mu.Unlock()
	return cnt
}
