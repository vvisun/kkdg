package kkbuffer

import (
	"math/bits"
	"sync"
	"sync/atomic"
)

// bfPool represents byte buffer pool.
//
// Distinct pools may be used for distinct types of byte buffers.
// Properly determined byte buffer types with their own pools may help reducing
// memory waste.
type bfPool struct {
	calls       [steps]uint64
	calibrating uint64
	defaultSize uint64
	pool        sync.Pool
}

// Get returns new byte buffer with zero length.
//
// Return it via Put to minimize GC overhead. For known expected size,
// use GetWithCapacity to reduce reallocations.
func (p *bfPool) Get() *ByteBuffer {
	v := p.pool.Get()
	if v != nil {
		b := v.(*ByteBuffer)
		b.released.Store(false)
		return b
	}
	if atomic.LoadUint64(&p.defaultSize) < minItemSize {
		atomic.StoreUint64(&p.defaultSize, minItemSize)
	}
	return &ByteBuffer{
		B: make([]byte, 0, int(p.defaultSize)),
	}
}

// GetWithCap returns a buffer with at least the specified capacity.
// Use when the expected size is known to reduce reallocations.
func (p *bfPool) GetWithCap(capacity int) *ByteBuffer {
	v := p.pool.Get()
	if v != nil {
		b := v.(*ByteBuffer)
		b.released.Store(false)
		if cap(b.B) < capacity {
			// If pooled buffer is too small, create a new one with required capacity
			b.B = make([]byte, 0, capacity)
		} else {
			b.B = b.B[:0]
		}
		return b
	}
	defaultSize := int(atomic.LoadUint64(&p.defaultSize))
	if defaultSize > capacity {
		capacity = defaultSize
	}
	return &ByteBuffer{
		B: make([]byte, 0, capacity),
	}
}

// Put releases byte buffer obtained via Get to the pool.
//
// The buffer mustn't be accessed after returning to the pool.
func (p *bfPool) Put(b *ByteBuffer) {
	if b == nil {
		return
	}
	if !b.released.CompareAndSwap(false, true) {
		return //防止重复释放
	}
	if cap(b.B) > maxItemSize {
		return // 超大 buffer 丢弃，不参与校准统计
	}
	idx := index(len(b.B))
	if atomic.AddUint64(&p.calls[idx], 1) > calibrateCallsThreshold {
		p.calibrate()
	}
	b.Reset()
	p.pool.Put(b)
}

func (p *bfPool) calibrate() {
	if !atomic.CompareAndSwapUint64(&p.calibrating, 0, 1) {
		return
	}

	var chooseIndex uint64
	var callsSum uint64
	for i := uint64(0); i < steps; i++ {
		calls := atomic.SwapUint64(&p.calls[i], 0)
		if calls > callsSum {
			callsSum = calls
			chooseIndex = i
		}
	}

	defaultSize := minItemSize << chooseIndex

	if defaultSize < minItemSize {
		defaultSize = minItemSize
	}

	atomic.StoreUint64(&p.defaultSize, uint64(defaultSize))
	atomic.StoreUint64(&p.calibrating, 0)
}

func index(n int) int {
	if n <= 0 {
		return 0
	}
	k := (n - 1) >> minBitSize
	idx := bits.Len(uint(k))
	if idx >= steps {
		return steps - 1
	}
	return idx
}
