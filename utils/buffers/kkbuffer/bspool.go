package kkbuffer

import (
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/utils/kklog"
)

type bsPool struct {
	pools       [32]sync.Pool
	defaultSize uint32
	count       int64
}

func NewBSPool(defaultSize uint32) *bsPool {
	var p = &bsPool{
		defaultSize: defaultSize,
	}
	return p
}

func (p *bsPool) Get() *ByteBuffer {
	idx := indexBS(p.defaultSize) //从向上取整的bucket中获取，避免需要扩容或cap不足
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
	idx := indexBS(uint32(capacity)) //从向上取整的bucket中获取，避免需要扩容或cap不足
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
		kklog.Debug("byte buffer already released")
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
	if size != 1<<idx {
		idx-- // 非精确 2^k 大小，放到前一个区间, 否则 GetWithCap 可能从太小的 bucket 里拿, 导致需要扩容或cap不足
	}
	atomic.AddInt64(&p.count, 1)
	p.pools[idx].Put(b)
}
