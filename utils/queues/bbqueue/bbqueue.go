package bbqueue

import (
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// BBQueue is a fixed-size FIFO queue that uses a circular buffer to store elements.
type BBQueue struct {
	buf       [][]*kkbuffer.ByteBuffer
	chunkSize int
	capacity  int
	headChunk int
	headPos   int
	tailChunk int
	tailPos   int
	count     int
	isStrict  bool //是否严格容量控制。true时，队列满时返回false，false时，队列满时自动扩容。
}

// NewBBQueue creates a queue where the `chunkSize` parameter means
// **the number of slots in each chunk**.
//
// The initial capacity is 1 chunk (i.e. `chunkSize`).
//
// isStrict: whether to strictly control the capacity. true时，队列满时返回false，false时，队列满时自动扩容。
func NewBBQueue(chunkSize int, isStrict bool) *BBQueue {
	if chunkSize <= 0 {
		chunkSize = 64
	}
	capacity := chunkSize
	chunks := 1
	q := &BBQueue{
		buf:       make([][]*kkbuffer.ByteBuffer, chunks),
		chunkSize: chunkSize,
		capacity:  capacity,
		isStrict:  isStrict,
	}
	for i := range q.buf {
		q.buf[i] = make([]*kkbuffer.ByteBuffer, chunkSize)
	}
	return q
}

func (q *BBQueue) Len() int {
	return q.count
}

func (q *BBQueue) IsFull() bool {
	return q.count == q.capacity
}

func (q *BBQueue) IsEmpty() bool {
	return q.count == 0
}

// 入队。严格模式下，队列满时返回false；非严格模式下，队列满时自动扩容。
func (q *BBQueue) Push(bb *kkbuffer.ByteBuffer) bool {
	if q.count == q.capacity {
		if q.isStrict {
			return false
		}
		q.grow()
	}
	// BBQueue 本身非并发安全；这里做一次取模归一化，避免并发误用时 panic。
	l := len(q.buf)
	tc := q.tailChunk
	if l > 0 && tc >= l {
		tc = tc % l
		q.tailChunk = tc
	}
	q.buf[tc][q.tailPos] = bb
	q.tailPos++
	if q.tailPos == q.chunkSize {
		q.tailPos = 0
		if l > 0 {
			q.tailChunk = (tc + 1) % l
		} else {
			q.tailChunk = 0
		}
	}
	q.count++
	return true
}

// 出队。队列空时返回nil。
func (q *BBQueue) Pop() *kkbuffer.ByteBuffer {
	if q.count == 0 {
		return nil
	}
	// BBQueue 本身非并发安全；这里做一次取模归一化，避免并发误用时 panic。
	l := len(q.buf)
	hc := q.headChunk
	if l > 0 && hc >= l {
		hc = hc % l
		q.headChunk = hc
	}
	bb := q.buf[hc][q.headPos]
	q.buf[hc][q.headPos] = nil
	q.headPos++
	if q.headPos == q.chunkSize {
		q.headPos = 0
		if l > 0 {
			q.headChunk = (hc + 1) % l
		} else {
			q.headChunk = 0
		}
	}
	q.count--
	if q.count == 0 {
		q.headChunk, q.headPos = 0, 0
		q.tailChunk, q.tailPos = 0, 0
	}
	return bb
}

// 批量弹出
// count: 需要弹出的数量
// recv: 接收缓冲区
// 返回实际弹出的数量
func (q *BBQueue) PopMany(count int, recv []*kkbuffer.ByteBuffer) int {
	if count <= 0 {
		return 0
	}
	if q.count == 0 || len(recv) == 0 {
		return 0
	}
	if count > len(recv) {
		count = len(recv)
	}
	if count > q.count {
		count = q.count
	}

	written := 0
	remain := count

	// BBQueue 本身非并发安全；这里做一次取模归一化，避免并发误用时 panic。
	l := len(q.buf)
	for remain > 0 {
		hc := q.headChunk
		if l > 0 && hc >= l {
			hc = hc % l
			q.headChunk = hc
		}
		// 本chunk剩余连续段
		n := q.chunkSize - q.headPos
		if remain < n {
			n = remain
		}

		copy(recv[written:written+n], q.buf[hc][q.headPos:q.headPos+n])
		clear(q.buf[hc][q.headPos : q.headPos+n])

		written += n
		remain -= n
		q.count -= n

		q.headPos += n
		if q.headPos == q.chunkSize {
			q.headPos = 0
			if l > 0 {
				q.headChunk = (hc + 1) % l
			} else {
				q.headChunk = 0
			}
		}
	}

	if q.count == 0 {
		q.headChunk, q.headPos = 0, 0
		q.tailChunk, q.tailPos = 0, 0
	}
	return written
}

// 扩容
func (q *BBQueue) grow() {
	// 只追加一个 chunk：不搬运元素，完全避免 copy。
	q.buf = append(q.buf, make([]*kkbuffer.ByteBuffer, q.chunkSize))
	q.capacity += q.chunkSize

	// 扩容后总槽位数变化，需要按新容量重算 tail，
	// 以保证从 head 走 count 步能到 tail（否则 full 状态下 tail==head 会导致覆盖）。
	headLinear := q.headChunk*q.chunkSize + q.headPos
	tailLinear := (headLinear + q.count) % q.capacity
	q.tailChunk = tailLinear / q.chunkSize
	q.tailPos = tailLinear % q.chunkSize
}
