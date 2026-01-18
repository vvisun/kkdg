package kkbuffer

import (
	"sync"
)

// bkPool represents byte buffer pool.
//
// 不同的池可以用于不同的字节缓冲区类型。
// 正确确定具有自己池的字节缓冲区类型可能有助于减少内存浪费。
type bkPool struct {
	minSize  int
	maxSize  int
	steps    int
	poolList []*sync.Pool
	sizeList []int
}

func NewBKPool(minSize int, maxSize int, steps int) *bkPool {
	if minSize < 64 {
		minSize = 64
	}
	if steps < 4 {
		steps = 8
	}
	if maxSize < minSize*steps {
		maxSize = minSize * steps
	}
	if maxSize > 1024*16 {
		maxSize = 1024 * 16
	}
	bk := &bkPool{
		poolList: make([]*sync.Pool, steps),
		minSize:  minSize,
		maxSize:  maxSize,
		steps:    steps,
		sizeList: make([]int, steps),
	}
	stepSize := (maxSize - minSize) / steps
	for i := 0; i < steps; i++ {
		size := minSize + stepSize*i
		if i == steps-1 {
			size = maxSize
		}
		bk.sizeList[i] = size
		bk.poolList[i] = &sync.Pool{
			New: func() interface{} {
				return &ByteBuffer{B: make([]byte, 0, size)}
			},
		}
	}
	return bk
}

func (p *bkPool) index(size int) int {
	for i := 0; i < p.steps; i++ {
		if size <= p.sizeList[i] {
			return i
		}
	}
	return p.steps - 1
}

// Get returns a byte buffer with the given size.
func (p *bkPool) GetWithCapacity(size int) *ByteBuffer {
	index := p.index(size)
	pool := p.poolList[index]
	b := pool.Get().(*ByteBuffer)
	b.released.Store(false)
	return b
}

func (p *bkPool) Put(b *ByteBuffer) {
	if !b.released.CompareAndSwap(false, true) {
		return //防止重复释放
	}
	index := p.index(cap(b.B))
	pool := p.poolList[index]
	b.Reset()
	pool.Put(b)
}
