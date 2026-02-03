package bbqueue

import (
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

const defaultSize = 128

// FIFO ring buffer queue.
type BBQueue struct {
	buf      []*kkbuffer.ByteBuffer
	head     int
	tail     int
	count    int
	isStrict bool //是否严格容量控制。true时，队列满时返回false，false时，队列满时自动扩容。
}

func NewBBQueue(size int, isStrict bool) *BBQueue {
	if size <= 0 {
		size = defaultSize
	}
	return &BBQueue{buf: make([]*kkbuffer.ByteBuffer, size), isStrict: isStrict}
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

func (q *BBQueue) PopMany(count int, recv []*kkbuffer.ByteBuffer, limitBytes int) int {
	if q.count == 0 {
		return 0 // 队列空，直接返回0
	}

	// 参数检查
	if count < 1 {
		count = 1 // 至少弹出1个
	}
	recvLen := len(recv)
	if recvLen == 0 {
		panic("recv is empty")
	}

	// 实际最多能弹出的数量
	if count > recvLen {
		count = recvLen
	}
	if count > q.count {
		count = q.count
	}

	written := 0
	totalBytes := 0

	for written < count && q.count > 0 {
		if limitBytes > 0 && written > 0 {
			willPop := q.buf[q.head]
			if willPop != nil {
				if totalBytes+willPop.Len() > limitBytes {
					break
				}
			}
		}

		bb := q.Pop()
		if bb == nil {
			written++
			continue
		}

		bbLen := bb.Len()
		recv[written] = bb
		written++
		totalBytes += bbLen

		// 当第一个元素就会超出 limitBytes 时，也允许弹出一个，防止 limitBytes 过小永远无法弹出。
		if limitBytes > 0 && totalBytes >= limitBytes && written > 0 {
			break
		}
	}

	return written

}

func (q *BBQueue) grow() {
	newSize := len(q.buf) * 2
	if newSize == 0 {
		newSize = defaultSize
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
	// kklog.Debugf("BBQueue grow: %.2fK -> %.2fK", float64(len(q.buf))/1024, float64(newSize)/1024)
}
