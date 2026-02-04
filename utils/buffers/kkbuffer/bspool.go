package kkbuffer

import (
	"math/bits"
	"sync"
	"sync/atomic"
)

func indexBS(n uint32) uint32 {
	return uint32(bits.Len32(n - 1))
}

type bsPool struct {
	pools       [32]sync.Pool
	defaultSize uint32
	count       int64
}

func (p *bsPool) Get() *ByteBuffer {
	idx := indexBS(p.defaultSize)
	v := p.pools[idx].Get()
	if v != nil {
		b := v.(*ByteBuffer)
		b.released.Store(false)
		b.B = b.B[:0]
		if atomic.LoadInt64(&p.count) > 0 {
			atomic.AddInt64(&p.count, -1)
		}
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
		b.B = b.B[:0]
		if atomic.LoadInt64(&p.count) > 0 {
			atomic.AddInt64(&p.count, -1)
		}
		return b
	}
	return &ByteBuffer{
		B: make([]byte, 0, capacity),
	}
}

func (p *bsPool) Put(b *ByteBuffer) {
	if b == nil {
		return // 空 buffer 直接丢弃
	}
	if !b.released.CompareAndSwap(false, true) {
		return // 防止重复释放
	}
	if atomic.LoadInt64(&p.count) > max_size_for_pool {
		return // 防止池过大耗尽内存
	}
	size := cap(b.B)
	if size < 1 || size > maxItemSize {
		return // 超大 buffer 丢弃，不参与校准统计
	}
	idx := indexBS(uint32(size))
	if size != 1<<idx { // this byte slice is not from Pool.Get(), put it into the previous interval of idx
		idx--
	}
	atomic.AddInt64(&p.count, 1)
	p.pools[idx].Put(b)
}
