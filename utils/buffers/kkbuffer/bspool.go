package kkbuffer

import (
	"math/bits"
	"sync"
)

func indexBS(n uint32) uint32 {
	return uint32(bits.Len32(n - 1))
}

type bsPool struct {
	pools       [32]sync.Pool
	defaultSize uint32
}

func (p *bsPool) Get() *ByteBuffer {
	idx := indexBS(p.defaultSize)
	v := p.pools[idx].Get()
	if v != nil {
		b := v.(*ByteBuffer)
		b.released.Store(false)
		return b
	}
	return &ByteBuffer{
		B: make([]byte, 0, p.defaultSize),
	}
}

func (p *bsPool) GetWithCap(capacity int) *ByteBuffer {
	if capacity <= 0 {
		return &ByteBuffer{
			B: make([]byte, 0),
		}
	}
	if capacity > maxItemSize {
		return &ByteBuffer{
			B: make([]byte, 0, capacity),
		}
	}
	idx := indexBS(uint32(capacity))
	v := p.pools[idx].Get()
	if v != nil {
		b := v.(*ByteBuffer)
		b.released.Store(false)
		return b
	}
	return &ByteBuffer{
		B: make([]byte, 0, capacity),
	}
}

func (p *bsPool) Put(b *ByteBuffer) {
	if b == nil {
		return
	}
	if !b.released.CompareAndSwap(false, true) {
		return
	}
	size := cap(b.B)
	if size < 1 || size > maxItemSize {
		return
	}
	idx := indexBS(uint32(size))
	if size != 1<<idx { // this byte slice is not from Pool.Get(), put it into the previous interval of idx
		idx--
	}
	p.pools[idx].Put(b)
}
