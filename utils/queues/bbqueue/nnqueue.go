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
	count    int  // 队列元素个数
	maxCount int  // 队列最大元素个数
	isStrict bool // 是否严格容量控制。true时，队列满时返回false，false时，队列满时自动扩容。
}

var _ IFiFoQueue = (*NNQueue)(nil)

func NewNNQueue(size int, isStrict bool) *NNQueue {
	return &NNQueue{head: nil, tail: nil, count: 0, maxCount: size, isStrict: isStrict}
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

func (q *NNQueue) Push(data *kkbuffer.ByteBuffer) bool {
	if q.isStrict && q.count >= q.maxCount {
		return false
	}
	node := getNNNode()
	node.data = data
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
	putNNNode(node)
	return bb
}

func (q *NNQueue) PopMany(count int, recv []*kkbuffer.ByteBuffer, limitBytes int) int {
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
			willPop := q.head.data
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
