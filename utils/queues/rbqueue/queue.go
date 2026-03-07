package rbqueue

import (
	"sync"
	"sync/atomic"
)

type ringBuffer[T any] struct {
	buffer []T
	head   int64
	tail   int64
	mod    int64
}

// Queue is a ring buffer queue: multi-producer, single consumer.
type Queue[T any] struct {
	len     int64
	content *ringBuffer[T]
	lock    sync.Mutex
}

func New[T any](initialSize int64) *Queue[T] {
	return &Queue[T]{
		content: &ringBuffer[T]{
			buffer: make([]T, initialSize),
			head:   0,
			tail:   0,
			mod:    initialSize,
		},
		len: 0,
	}
}

func (q *Queue[T]) Push(item T) {
	q.lock.Lock()
	c := q.content
	c.tail = (c.tail + 1) % c.mod
	if c.tail == c.head {
		var fillFactor int64 = 2
		newLen := c.mod * fillFactor
		newBuff := make([]T, newLen)

		for i := int64(0); i < c.mod; i++ {
			buffIndex := (c.tail + i) % c.mod
			newBuff[i] = c.buffer[buffIndex]
		}
		newContent := &ringBuffer[T]{
			buffer: newBuff,
			head:   0,
			tail:   c.mod,
			mod:    newLen,
		}
		q.content = newContent
	}
	atomic.AddInt64(&q.len, 1)
	q.content.buffer[q.content.tail] = item
	q.lock.Unlock()
}

// Pop removes and returns the next item. Returns (zero, false) when empty.
func (q *Queue[T]) Pop() (T, bool) {
	var zero T
	if q.Empty() {
		return zero, false
	}

	q.lock.Lock()
	c := q.content
	c.head = (c.head + 1) % c.mod
	res := c.buffer[c.head]
	c.buffer[c.head] = zero
	atomic.AddInt64(&q.len, -1)
	q.lock.Unlock()
	return res, true
}

// PopMany removes up to count items into buffer. Returns (nil, false) when empty or count <= 0.
// If buffer is too small, a new slice is allocated; otherwise buffer is reused and sliced to count.
func (q *Queue[T]) PopMany(count int64, buffer []T) ([]T, bool) {
	if q.Empty() || count <= 0 {
		return nil, false
	}

	q.lock.Lock()
	c := q.content

	if count > q.len {
		count = q.len
	}
	atomic.AddInt64(&q.len, -count)

	var zero T
	if int(count) > len(buffer) {
		buffer = make([]T, count)
	} else {
		buffer = buffer[:count]
	}

	md := c.mod
	for i := int64(0); i < count; i++ {
		pos := (c.head + 1 + i) % md
		buffer[i] = c.buffer[pos]
		c.buffer[pos] = zero
	}
	c.head = (c.head + count) % md

	q.lock.Unlock()
	return buffer, true
}

func (q *Queue[T]) Length() int64 {
	return atomic.LoadInt64(&q.len)
}

func (q *Queue[T]) Empty() bool {
	return q.Length() == 0
}
