package bbqueue

import "github.com/vvisun/kkdg/utils/buffers/kkbuffer"

type IFiFoQueue interface {
	Len() int
	IsFull() bool
	IsEmpty() bool
	Push(data *kkbuffer.ByteBuffer) bool
	Pop() *kkbuffer.ByteBuffer
	PopMany(count int, recv []*kkbuffer.ByteBuffer, limitBytes int) int
}
