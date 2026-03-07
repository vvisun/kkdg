package kkmpmc

import (
	"sync/atomic"
)

// node 链表节点，仅内部使用。
type node[T any] struct {
	next  atomic.Pointer[node[T]]
	value *T
}

// Queue 多生产者多消费者无锁队列，基于链表，不限制容量。
// 多个 goroutine 可并发 Push，多个 goroutine 可并发调用 Pop。
type Queue[T any] struct {
	head   atomic.Pointer[node[T]] // 哨兵节点，首个元素在 head.next，多消费者 CAS 竞争
	tail   atomic.Pointer[node[T]] // 队尾节点，多生产者 CAS 竞争
	length atomic.Int64            // 当前长度，供 Len() 观测
}

// NewQueue 创建无界 MPMC 队列。
func NewQueue[T any]() *Queue[T] {
	dummy := &node[T]{}
	q := &Queue[T]{}
	q.head.Store(dummy)
	q.tail.Store(dummy)
	return q
}

// Push 由任意生产者调用，将 item 入队，永不因容量失败。
// 多 goroutine 可并发调用。
func (q *Queue[T]) Push(item *T) {
	n := &node[T]{value: item}
	for {
		tail := q.tail.Load()
		if tail.next.CompareAndSwap(nil, n) {
			q.tail.CompareAndSwap(tail, n)
			q.length.Add(1)
			return
		}
		q.tail.CompareAndSwap(tail, tail.next.Load())
	}
}

// Pop 由任意消费者调用，出队一个元素。队列空时返回 (nil, false)。
// 多 goroutine 可并发调用。
func (q *Queue[T]) Pop() (*T, bool) {
	for {
		head := q.head.Load()
		next := head.next.Load()
		if next == nil {
			return nil, false
		}
		if q.head.CompareAndSwap(head, next) {
			q.length.Add(-1)
			return next.value, true
		}
	}
}

// Len 返回当前队列中的元素个数（近似值，仅用于观测）。
func (q *Queue[T]) Len() int {
	return int(q.length.Load())
}

// IsEmpty 返回队列是否为空。
func (q *Queue[T]) IsEmpty() bool {
	return q.head.Load().next.Load() == nil
}
