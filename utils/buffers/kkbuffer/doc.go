// Package kkbuffer provides byte buffer and pool for minimizing allocations.
//
// Quick start:
//
//	buf := kkbuffer.Get()
//	buf.WriteString("hello")
//	defer kkbuffer.Put(buf)
//
// Performance tips:
//
//   - Known size: use GetWithCapacity(n), or call Grow(n) before a batch of
//     Write/WriteByte/WriteString to avoid repeated reallocations.
//   - Many WriteByte in a loop: call Grow(b.Len()+n) first, then write n bytes.
//   - Repeated Set/SetString: GetWithCapacity or Grow before Reset+Set keeps
//     cap >= len(data), so Set uses copy instead of allocating.
package kkbuffer

const (
	minBitSize = 6  // 2**6=64 是CPU缓存行大小
	steps      = 16 // 16个步长 = 2^6 * 2^16 = 65536 = 64KB

	minItemSize = 1 << minBitSize               // 64
	maxItemSize = 1 << (minBitSize + steps - 1) // 2^6 * 2^16 = 65536 = 64KB
)

var calibrateCallsThreshold uint64 = 128 // 多少次调用后进行校准
var defaultPool bsPool

func init() {
	defaultPool.defaultSize = 128
}

// Get returns an empty byte buffer from the pool.
//
// The buffer may be returned via Put to reduce allocations.
// When the expected size is known, prefer GetWithCapacity to avoid
// reallocations on first writes.
func Get() *ByteBuffer { return defaultPool.Get() }

// GetWithCapacity returns a buffer with at least the specified capacity.
//
// Prefer this over Get when the expected size is known, to avoid
// reallocations on the first Write/Set/SetString.
func GetWithCapacity(capacity int) *ByteBuffer {
	return defaultPool.GetWithCap(capacity)
}

// Put returns byte buffer to the pool.
//
// ByteBuffer.B mustn't be touched after returning it to the pool.
// Otherwise data races will occur.
func Put(b *ByteBuffer) { defaultPool.Put(b) }
