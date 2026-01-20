package kkbuffer

import (
	"math/bits"
	"sort"
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
	maxSize     uint64

	pool sync.Pool
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
	size := atomic.LoadUint64(&p.defaultSize)
	if size == 0 {
		size = minSize
	}
	return &ByteBuffer{
		B: make([]byte, 0, int(size)),
	}
}

// GetWithCapacity returns a buffer with at least the specified capacity.
// Use when the expected size is known to reduce reallocations.
func (p *bfPool) GetWithCapacity(capacity int) *ByteBuffer {
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
	if defaultSize == 0 {
		defaultSize = minSize
	}
	initCap := capacity
	if defaultSize > capacity {
		initCap = defaultSize
	}
	return &ByteBuffer{
		B: make([]byte, 0, initCap),
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
	maxSize := int(atomic.LoadUint64(&p.maxSize))
	if maxSize != 0 && cap(b.B) > maxSize {
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

	a := make(callSizes, 0, steps)
	var callsSum uint64
	for i := uint64(0); i < steps; i++ {
		calls := atomic.SwapUint64(&p.calls[i], 0)
		callsSum += calls
		a = append(a, callSize{
			calls: calls,
			size:  minSize << i,
		})
	}
	sort.Sort(a)

	defaultSize := a[0].size
	maxSize := defaultSize

	maxSum := uint64(float64(callsSum) * maxPercentile)
	callsSum = 0
	for i := 0; i < steps; i++ {
		if callsSum > maxSum {
			break
		}
		callsSum += a[i].calls
		size := a[i].size
		if size > maxSize {
			maxSize = size
		}
	}

	atomic.StoreUint64(&p.defaultSize, defaultSize)
	atomic.StoreUint64(&p.maxSize, maxSize)

	atomic.StoreUint64(&p.calibrating, 0)
}

//--------------------------------------------------

type callSize struct {
	calls uint64
	size  uint64
}

type callSizes []callSize

func (ci callSizes) Len() int {
	return len(ci)
}

func (ci callSizes) Less(i, j int) bool {
	return ci[i].calls > ci[j].calls
}

func (ci callSizes) Swap(i, j int) {
	ci[i], ci[j] = ci[j], ci[i]
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
