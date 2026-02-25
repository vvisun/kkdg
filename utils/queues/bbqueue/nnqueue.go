package bbqueue

import "sync"

type nnNode[T Sizable] struct {
	next *nnNode[T]
	data T
}

// NNQueue 链表实现，泛型版本，内存占用更低，但性能不如BBQueue。
type NNQueue[T Sizable] struct {
	head     *nnNode[T]
	tail     *nnNode[T]
	free     *nnNode[T]
	count    int
	maxCount int
	isStrict bool

	freeCount int
	freeMax   int

	pool *sync.Pool
}

// NewNNQueue 创建泛型 NNQueue
func NewNNQueue[T Sizable](size int, isStrict bool) *NNQueue[T] {
	if size <= 0 {
		size = defaultSize
	}
	freeMax := size
	if freeMax < 64 {
		freeMax = 64
	}
	if freeMax > 4096 {
		freeMax = 4096
	}
	return &NNQueue[T]{
		head:     nil,
		tail:     nil,
		free:     nil,
		count:    0,
		maxCount: size,
		isStrict: isStrict,
		freeMax:  freeMax,
		pool: &sync.Pool{
			New: func() interface{} { return &nnNode[T]{} },
		},
	}
}

func (q *NNQueue[T]) Cap() int {
	return q.maxCount
}

func (q *NNQueue[T]) Len() int {
	return q.count
}

func (q *NNQueue[T]) IsFull() bool {
	return q.isStrict && q.count >= q.maxCount
}

func (q *NNQueue[T]) IsEmpty() bool {
	return q.count == 0
}

func (q *NNQueue[T]) acquireNode() *nnNode[T] {
	if q.free != nil {
		n := q.free
		q.free = n.next
		q.freeCount--
		n.next = nil
		return n
	}
	return q.pool.Get().(*nnNode[T])
}

func (q *NNQueue[T]) recycleNode(node *nnNode[T]) {
	if node == nil {
		return
	}
	var zero T
	node.data = zero
	if q.freeCount < q.freeMax {
		node.next = q.free
		q.free = node
		q.freeCount++
		return
	}
	q.pool.Put(node)
}

func (q *NNQueue[T]) Push(v T) bool {
	if q.isStrict && q.count >= q.maxCount {
		return false
	}
	node := q.acquireNode()
	node.data = v
	node.next = nil
	if q.head == nil {
		q.head = node
		q.tail = node
	} else {
		q.tail.next = node
		q.tail = node
	}
	q.count++
	return true
}

func (q *NNQueue[T]) Pop() T {
	if q.head == nil {
		var zero T
		return zero
	}
	node := q.head
	q.head = node.next
	if q.head == nil {
		q.tail = nil
	}
	q.count--
	bb := node.data
	q.recycleNode(node)
	return bb
}

func (q *NNQueue[T]) PopMany(count int, recv []T, limitBytes int) int {
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

	node := q.head
	for written < count && node != nil {
		if limitBytes > 0 && written > 0 {
			willPop := node.data
			if sizeOf(willPop) > 0 && totalBytes+sizeOf(willPop) > limitBytes {
				break
			}
		}

		next := node.next
		v := node.data
		recv[written] = v
		written++
		totalBytes += sizeOf(v)

		q.count--
		q.recycleNode(node)
		node = next

		if limitBytes > 0 && totalBytes >= limitBytes && written > 0 {
			break
		}
	}

	q.head = node
	if q.head == nil {
		q.tail = nil
	}

	return written
}
