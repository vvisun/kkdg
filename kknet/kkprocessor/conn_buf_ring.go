package kkprocessor

import (
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

type connBuf struct {
	connID kknet.CONN_ID
	buf    *kkbuffer.ByteBuffer
}

// connBufRing 环形队列，存 connBuf
type connBufRing struct {
	buf      []connBuf
	head     int
	tail     int
	count    int
	capacity int
	strict   bool
}

func newConnBufRing(size int, strict bool) *connBufRing {
	if size <= 0 {
		size = 128
	}
	return &connBufRing{
		buf:      make([]connBuf, size),
		capacity: size,
		strict:   strict,
	}
}

func (q *connBufRing) Len() int {
	return q.count
}

func (q *connBufRing) IsEmpty() bool {
	return q.count == 0
}

func (q *connBufRing) IsFull() bool {
	return q.strict && q.count >= q.capacity
}

func (q *connBufRing) Push(cb connBuf) bool {
	if q.strict && q.count >= q.capacity {
		return false
	}
	if q.count >= q.capacity {
		q.grow()
	}
	q.buf[q.tail] = cb
	q.tail = (q.tail + 1) % q.capacity
	q.count++
	return true
}

func (q *connBufRing) grow() {
	newCap := q.capacity * 2
	if newCap > 65536 {
		newCap = 65536
	}
	newBuf := make([]connBuf, newCap)
	for i := 0; i < q.count; i++ {
		pos := (q.head + i) % q.capacity
		newBuf[i] = q.buf[pos]
	}
	q.buf = newBuf
	q.head = 0
	q.tail = q.count
	q.capacity = newCap
}

func (q *connBufRing) Pop() (connBuf, bool) {
	if q.count == 0 {
		return connBuf{}, false
	}
	cb := q.buf[q.head]
	q.buf[q.head] = connBuf{}
	q.head = (q.head + 1) % q.capacity
	q.count--
	return cb, true
}

func (q *connBufRing) Peek() (connBuf, bool) {
	if q.count == 0 {
		return connBuf{}, false
	}
	return q.buf[q.head], true
}

func (q *connBufRing) PopMany(dst []connBuf, limitBytes int) int {
	if q.count == 0 || len(dst) == 0 {
		return 0
	}
	n := 0
	totalBytes := 0
	for n < len(dst) && q.count > 0 {
		pos := (q.head + n) % q.capacity
		cb := q.buf[pos]
		if limitBytes > 0 && n > 0 {
			if cb.buf != nil && totalBytes+cb.buf.Len() > limitBytes {
				break
			}
		}
		dst[n] = cb
		q.buf[pos] = connBuf{}
		n++
		if cb.buf != nil {
			totalBytes += cb.buf.Len()
		}
	}
	q.head = (q.head + n) % q.capacity
	q.count -= n
	return n
}

func (q *connBufRing) DrainAndRelease(release func(*kkbuffer.ByteBuffer)) {
	for q.count > 0 {
		cb, _ := q.Pop()
		if cb.buf != nil && release != nil {
			release(cb.buf)
		}
	}
}
