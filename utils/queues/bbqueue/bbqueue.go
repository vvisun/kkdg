package bbqueue

import (
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

type BBQueue struct {
	buf      []*kkbuffer.ByteBuffer
	head     int
	tail     int
	count    int
	isStrict bool //是否严格容量控制。true时，队列满时返回false，false时，队列满时自动扩容。
}

func NewBBQueue(size int, isStrict bool) BBQueue {
	if size <= 0 {
		size = 64
	}
	return BBQueue{buf: make([]*kkbuffer.ByteBuffer, size), isStrict: isStrict}
}

func (q *BBQueue) Len() int {
	return q.count
}

func (q *BBQueue) IsFull() bool {
	return q.count == len(q.buf)
}

func (q *BBQueue) IsEmpty() bool {
	return q.count == 0
}

func (q *BBQueue) Push(bb *kkbuffer.ByteBuffer) bool {
	if q.count == len(q.buf) {
		if q.isStrict {
			return false
		}
		q.grow()
	}
	q.buf[q.tail] = bb
	q.tail = (q.tail + 1) % len(q.buf)
	q.count++
	return true
}

func (q *BBQueue) Pop() *kkbuffer.ByteBuffer {
	if q.count == 0 {
		return nil
	}
	bb := q.buf[q.head]
	q.buf[q.head] = nil
	q.head = (q.head + 1) % len(q.buf)
	q.count--
	if q.count == 0 {
		q.head = 0
		q.tail = 0
	}
	return bb
}

func (q *BBQueue) grow() {
	newSize := len(q.buf) * 2
	if newSize == 0 {
		newSize = 64
	}
	newQueue := make([]*kkbuffer.ByteBuffer, newSize)
	if q.count > 0 {
		if q.head < q.tail {
			copy(newQueue, q.buf[q.head:q.tail])
		} else {
			n := copy(newQueue, q.buf[q.head:])
			copy(newQueue[n:], q.buf[:q.tail])
		}
	}
	q.buf = newQueue
	q.head = 0
	q.tail = q.count
}
