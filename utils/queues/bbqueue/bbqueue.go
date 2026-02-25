package bbqueue

import "reflect"

const defaultSize = 128
const shrinkMinSize = 2048
const enableShrink = false

// BBQueue FIFO 环形队列，泛型实现
type BBQueue[T Sizable] struct {
	buf      []T
	head     int
	tail     int
	count    int
	isStrict bool
}

// NewBBQueue 创建泛型 BBQueue
func NewBBQueue[T Sizable](size int, isStrict bool) *BBQueue[T] {
	if size <= 0 {
		size = defaultSize
	}
	return &BBQueue[T]{buf: make([]T, size), isStrict: isStrict}
}

func (q *BBQueue[T]) Cap() int {
	return len(q.buf)
}

func (q *BBQueue[T]) Len() int {
	return q.count
}

func (q *BBQueue[T]) IsFull() bool {
	return q.count == len(q.buf)
}

func (q *BBQueue[T]) IsEmpty() bool {
	return q.count == 0
}

func (q *BBQueue[T]) Push(v T) bool {
	if q.count == len(q.buf) {
		if q.isStrict {
			return false
		}
		q.grow()
	}
	q.buf[q.tail] = v
	q.tail = (q.tail + 1) % len(q.buf)
	q.count++
	return true
}

func (q *BBQueue[T]) Pop() T {
	if q.count == 0 {
		var zero T
		return zero
	}
	v := q.buf[q.head]
	q.buf[q.head] = zeroOf[T]()
	q.head = (q.head + 1) % len(q.buf)
	q.count--
	if q.count == 0 {
		q.head = 0
		q.tail = 0
	}
	if enableShrink && !q.isStrict {
		q.shrink()
	}
	return v
}

func zeroOf[T any]() T { var z T; return z }

func (q *BBQueue[T]) PopMany(count int, recv []T, limitBytes int) int {
	if q.count == 0 {
		return 0
	}
	recvLen := len(recv)
	if recvLen == 0 {
		panic("recv is empty")
	}
	if count < 1 {
		count = 1
	}
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
			if sizeOf(willPop) > 0 && totalBytes+sizeOf(willPop) > limitBytes {
				break
			}
		}

		v := q.Pop()
		recv[written] = v
		written++
		totalBytes += sizeOf(v)

		if limitBytes > 0 && totalBytes >= limitBytes && written > 0 {
			break
		}
	}

	return written
}

func sizeOf[T Sizable](v T) int {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Pointer && rv.IsNil() {
		return 0
	}
	return v.Len()
}

func (q *BBQueue[T]) grow() {
	newSize := len(q.buf) * 2
	if newSize == 0 {
		newSize = defaultSize
	}
	newQueue := make([]T, newSize)
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

func (q *BBQueue[T]) shrink() {
	if q == nil || q.isStrict {
		return
	}
	cur := len(q.buf)
	if q.count >= cur/2 {
		return
	}
	if cur <= shrinkMinSize {
		return
	}
	newSize := cur / 2
	if newSize < shrinkMinSize {
		newSize = shrinkMinSize
	}
	if newSize < q.count {
		newSize = q.count
	}
	if newSize >= cur {
		return
	}
	newQueue := make([]T, newSize)
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
