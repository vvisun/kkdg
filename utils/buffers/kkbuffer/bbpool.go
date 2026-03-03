package kkbuffer

import (
	"sync"
)

// bbPool represents byte buffer pool.
//
// Distinct pools may be used for distinct types of byte buffers.
// Properly determined byte buffer types with their own pools may help reducing
// memory waste.
type bbPool struct {
	begin       int
	end         int
	defaultSize uint32
	shards      map[int]*sync.Pool
}

// newBBPool 创建一个内存池
// creates a memory pool
// left 和 right 表示内存池的区间范围，它们将被转换为 2 的 n 次幂
// left and right indicate the interval range of the memory pool, they will be transformed into pow(2, n)
// 小于 left 的情况下，Get 方法将返回至少 left 字节的缓冲区；大于 right 的情况下，Put 方法不会回收缓冲区
// Below left, the Get method will return at least left bytes; above right, the Put method will not reclaim the buffer
func NewBBPool(left, right uint32) *bbPool {
	var begin, end = int(binaryCeil(left)), int(binaryCeil(right))
	var p = &bbPool{
		begin:  begin,
		end:    end,
		shards: map[int]*sync.Pool{},
	}
	for i := begin; i <= end; i *= 2 {
		capacity := i
		p.shards[i] = &sync.Pool{
			New: func() any {
				return &ByteBuffer{
					B: make([]byte, 0, capacity),
				}
			},
		}
	}
	return p
}

// Put 将缓冲区放回到内存池
// returns the buffer to the memory pool
func (p *bbPool) Put(b *ByteBuffer) {
	if b != nil {
		if pool, ok := p.shards[b.Cap()]; ok {
			pool.Put(b)
		}
	}
}

// Get 从内存池中获取一个至少 n 字节的缓冲区
// fetches a buffer from the memory pool, of at least n bytes
func (p *bbPool) GetWithCap(n int) *ByteBuffer {
	var size = Max(int(binaryCeil(uint32(n))), p.begin)
	if pool, ok := p.shards[size]; ok {
		b := pool.Get().(*ByteBuffer)
		if b.Cap() < size {
			b.grow(size)
		}
		b.Reset()
		return b
	}
	return &ByteBuffer{
		B: make([]byte, 0, n),
	}
}
