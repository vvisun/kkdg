package bbqueue

import (
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

const bbQueueChunkSize = 64

type BBQueue struct {
	buf      [][]*kkbuffer.ByteBuffer
	capacity int
	head     int
	tail     int
	count    int
	isStrict bool //是否严格容量控制。true时，队列满时返回false，false时，队列满时自动扩容。
}

func NewBBQueue(size int, isStrict bool) BBQueue {
	if size <= 0 {
		size = 64
	}
	q := BBQueue{
		buf:      make([][]*kkbuffer.ByteBuffer, (size+bbQueueChunkSize-1)/bbQueueChunkSize),
		capacity: size,
		isStrict: isStrict,
	}
	for i := range q.buf {
		q.buf[i] = make([]*kkbuffer.ByteBuffer, bbQueueChunkSize)
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

func (q *BBQueue) Push(bb *kkbuffer.ByteBuffer) bool {
	if q.count == q.capacity {
		if q.isStrict {
			return false
		}
		q.grow()
	}
	chunk := q.tail / bbQueueChunkSize
	pos := q.tail % bbQueueChunkSize
	q.buf[chunk][pos] = bb
	q.tail = (q.tail + 1) % q.capacity
	q.count++
	return true
}

func (q *BBQueue) Pop() *kkbuffer.ByteBuffer {
	if q.count == 0 {
		return nil
	}
	chunk := q.head / bbQueueChunkSize
	pos := q.head % bbQueueChunkSize
	bb := q.buf[chunk][pos]
	q.buf[chunk][pos] = nil
	q.head = (q.head + 1) % q.capacity
	q.count--
	if q.count == 0 {
		q.head = 0
		q.tail = 0
	}
	return bb
}

// 扩容
func (q *BBQueue) grow() {
	oldCap := q.capacity
	newCap := oldCap * 2
	if newCap == 0 {
		newCap = 64
	}

	newChunkCount := (newCap + bbQueueChunkSize - 1) / bbQueueChunkSize
	newBuf := make([][]*kkbuffer.ByteBuffer, newChunkCount)

	// 把现有元素按队列顺序搬到新buf的前面。为了避免大规模copy：
	// - 以chunk为单位尽量复用整块(仅复制slice header)；
	// - 只有头尾最多两个chunk会落在非对齐位置，需要小范围copy。
	if q.count > 0 {
		dstIdx := 0
		srcIdx := q.head
		remain := q.count

		for remain > 0 {
			dstChunk := dstIdx / bbQueueChunkSize
			dstPos := dstIdx % bbQueueChunkSize
			srcChunk := srcIdx / bbQueueChunkSize
			srcPos := srcIdx % bbQueueChunkSize

			// 尽量复用整块chunk（要求源/目标都chunk对齐，且源chunk完全落在oldCap范围内）
			if dstPos == 0 && srcPos == 0 && remain >= bbQueueChunkSize && srcIdx+bbQueueChunkSize <= oldCap {
				newBuf[dstChunk] = q.buf[srcChunk]
				dstIdx += bbQueueChunkSize
				srcIdx += bbQueueChunkSize
				remain -= bbQueueChunkSize
				if srcIdx == oldCap {
					srcIdx = 0
				}
				continue
			}

			if newBuf[dstChunk] == nil {
				newBuf[dstChunk] = make([]*kkbuffer.ByteBuffer, bbQueueChunkSize)
			}

			n := bbQueueChunkSize - dstPos
			if t := bbQueueChunkSize - srcPos; t < n {
				n = t
			}
			if t := oldCap - srcIdx; t < n {
				n = t
			}
			if remain < n {
				n = remain
			}

			copy(newBuf[dstChunk][dstPos:dstPos+n], q.buf[srcChunk][srcPos:srcPos+n])

			dstIdx += n
			srcIdx += n
			remain -= n
			if srcIdx == oldCap {
				srcIdx = 0
			}
		}
	}

	// 补齐未分配的chunk
	for i := range newBuf {
		if newBuf[i] == nil {
			newBuf[i] = make([]*kkbuffer.ByteBuffer, bbQueueChunkSize)
		}
	}

	q.buf = newBuf
	q.capacity = newCap
	q.head = 0
	q.tail = q.count
}
