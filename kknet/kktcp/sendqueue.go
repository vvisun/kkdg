package kktcp

import "github.com/vvisun/kkdg/utils/buffers/kkbuffer"

type sendQueue struct {
	buf   []*kkbuffer.ByteBuffer
	head  int
	tail  int
	count int
}

func newSendQueue(size int) sendQueue {
	if size <= 0 {
		size = 64
	}
	return sendQueue{buf: make([]*kkbuffer.ByteBuffer, size)}
}

func (q *sendQueue) Len() int {
	return q.count
}

func (q *sendQueue) Push(bb *kkbuffer.ByteBuffer) {
	if q.count == len(q.buf) {
		q.grow()
	}
	q.buf[q.tail] = bb
	q.tail = (q.tail + 1) % len(q.buf)
	q.count++
}

func (q *sendQueue) Pop() *kkbuffer.ByteBuffer {
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

func (q *sendQueue) grow() {
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
