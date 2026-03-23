package bbqueue

import (
	"sync"

	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

type nnNode struct {
	next *nnNode
	data *kkbuffer.ByteBuffer
}

var nnNodePool = sync.Pool{
	New: func() interface{} {
		return &nnNode{next: nil, data: nil}
	},
}

func getNNNode() *nnNode {
	return nnNodePool.Get().(*nnNode)
}

func putNNNode(node *nnNode) {
	node.next = nil
	node.data = nil
	nnNodePool.Put(node)
}

type NNQueue struct {
	head     *nnNode
	tail     *nnNode
	free     *nnNode // per-queue freelist (LIFO)
	count    int     // 队列元素个数
	maxCount int     // 队列最大元素个数
	isStrict bool    // 是否严格容量控制。true时，队列满时返回false，false时，队列满时自动扩容。

	freeCount int // freelist nodes count
	freeMax   int // freelist cap; overflow returns to global pool
}

var _ IFiFoQueue = (*NNQueue)(nil)

// 链表实现，内存占用更低，但性能不如BBQueue。
func NewNNQueue(size int, isStrict bool) *NNQueue {
	if size <= 0 {
		size = defaultSize
	}
	// freelist cap: keep a small bounded cache per-queue to avoid sync.Pool overhead
	// while still limiting retained nodes in low-memory mode.
	freeMax := size
	if freeMax < 64 {
		freeMax = 64
	}
	if freeMax > 256 {
		freeMax = 256
	}
	return &NNQueue{
		head:     nil,
		tail:     nil,
		free:     nil,
		count:    0,
		maxCount: size,
		isStrict: isStrict,
		freeMax:  freeMax,
	}
}

func (q *NNQueue) Len() int {
	return q.count
}

func (q *NNQueue) IsFull() bool {
	return q.isStrict && q.count >= q.maxCount
}

func (q *NNQueue) IsEmpty() bool {
	return q.count == 0
}

func (q *NNQueue) acquireNode() *nnNode {
	if q.free != nil {
		n := q.free
		q.free = n.next
		q.freeCount--
		n.next = nil
		return n
	}
	return getNNNode()
}

func (q *NNQueue) recycleNode(node *nnNode) {
	if node == nil {
		return
	}
	node.data = nil
	if q.freeCount < q.freeMax {
		node.next = q.free
		q.free = node
		q.freeCount++
		return
	}
	putNNNode(node)
}

func (q *NNQueue) Push(data *kkbuffer.ByteBuffer) bool {
	if q.isStrict && q.count >= q.maxCount {
		return false
	}
	node := q.acquireNode()
	node.data = data
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

func (q *NNQueue) Pop() *kkbuffer.ByteBuffer {
	if q.head == nil {
		return nil
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

func (q *NNQueue) PopMany(count int, recv []*kkbuffer.ByteBuffer, limitBytes int) int {
	if q.count == 0 {
		return 0 // 队列空，直接返回0
	}

	recvLen := len(recv)
	if recvLen == 0 {
		panic("recv is empty")
	}

	if count < 1 {
		count = 1 // 至少弹出1个
	}
	if count > recvLen {
		count = recvLen
	}
	if count > q.count {
		count = q.count
	}

	written := 0
	totalBytes := 0

	// Batch detach nodes from head, avoid per-item Pop() overhead.
	node := q.head
	for written < count && node != nil {
		if limitBytes > 0 && written > 0 {
			willPop := node.data
			if willPop != nil && totalBytes+willPop.Len() > limitBytes {
				break
			}
		}

		next := node.next
		bb := node.data
		recv[written] = bb
		written++
		if bb != nil {
			totalBytes += bb.Len()
		}

		q.count--
		q.recycleNode(node)
		node = next

		// 当第一个元素就会超出 limitBytes 时，也允许弹出一个，防止 limitBytes 过小永远无法弹出。
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
