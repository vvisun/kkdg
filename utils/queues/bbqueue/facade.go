package bbqueue

import "github.com/vvisun/kkdg/utils/buffers/kkbuffer"

type IFiFoQueue interface {
	// return the number of items in the queue.
	Len() int
	// return true if the queue is full.
	IsFull() bool
	// return true if the queue is empty.
	IsEmpty() bool
	// push an item to the queue.
	// return true if the item is pushed successfully, false if the queue is full.
	Push(data *kkbuffer.ByteBuffer) bool
	// pop an item from the queue.
	// return the item if the queue is not empty, nil if the queue is empty.
	Pop() *kkbuffer.ByteBuffer
	// pop many items from queue, items will be stored in the given recv array.
	// if limitBytes is greater than 0, the total bytes of items will not exceed limitBytes.
	// return the number of items popped.
	PopMany(count int, recv []*kkbuffer.ByteBuffer, limitBytes int) int
}
