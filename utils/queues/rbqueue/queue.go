package rbqueue

import (
	"sync"
	"sync/atomic"
)

type ringBuffer struct {
	buffer []interface{}
	head   int64
	tail   int64
	mod    int64
}

// ring buffer queue
// multi-producer, single consumer
type Queue struct {
	len     int64
	content *ringBuffer
	lock    sync.Mutex
}

func New(initialSize int64) *Queue {
	return &Queue{
		content: &ringBuffer{
			buffer: make([]interface{}, initialSize),
			head:   0,
			tail:   0,
			mod:    initialSize,
		},
		len: 0,
	}
}

func (q *Queue) Push(item interface{}) {
	q.lock.Lock()
	c := q.content
	c.tail = (c.tail + 1) % c.mod
	if c.tail == c.head {
		var fillFactor int64 = 2
		// we need to resize

		newLen := c.mod * fillFactor
		newBuff := make([]interface{}, newLen)

		for i := int64(0); i < c.mod; i++ {
			buffIndex := (c.tail + i) % c.mod
			newBuff[i] = c.buffer[buffIndex]
		}
		// set the new buffer and reset head and tail
		newContent := &ringBuffer{
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

// single consumer
func (q *Queue) Pop() (interface{}, bool) {
	if q.Empty() {
		return nil, false
	}
	// as we are a single consumer,
	// no other thread can have poped the items there are guaranteed to be items now

	q.lock.Lock()
	c := q.content
	c.head = (c.head + 1) % c.mod
	res := c.buffer[c.head]
	c.buffer[c.head] = nil
	atomic.AddInt64(&q.len, -1)
	q.lock.Unlock()
	return res, true
}

func (q *Queue) PopMany(count int64, buffer []interface{}) ([]interface{}, bool) {
	if q.Empty() || count <= 0 {
		return nil, false
	}

	q.lock.Lock()
	c := q.content

	if count > q.len {
		count = q.len
	}
	atomic.AddInt64(&q.len, -count)

	if len(buffer) < int(count) {
		buffer = make([]interface{}, count)
	} else {
		buffer = buffer[:count]
	}

	md := c.mod
	for i := int64(0); i < count; i++ {
		pos := (c.head + 1 + i) % md
		buffer[i] = c.buffer[pos]
		c.buffer[pos] = nil
	}
	c.head = (c.head + count) % md

	q.lock.Unlock()
	return buffer, true
}

func (q *Queue) Length() int64 {
	return atomic.LoadInt64(&q.len)
}

func (q *Queue) Empty() bool {
	return q.Length() == 0
}
