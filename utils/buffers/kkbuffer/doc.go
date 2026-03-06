package kkbuffer

const (
	minBitSize = 6  // 2**6=64 是CPU缓存行大小
	steps      = 16 // 16个步长 = 2^6 * 2^16 = 65536 = 64KB

	minItemSize = 1 << minBitSize               // 64
	maxItemSize = 1 << (minBitSize + steps - 1) // 2^6 * 2^16 = 65536 = 64KB

	// 假设平均每个对象1KB，1024个对象就是1MB。
	// 所以必须做限制，防止池过大耗尽内存
	// 使用最频繁，这里略微放宽一些。
	max_size_for_pool = 80 * 1024 //假设每个对象1KB，80*1024个对象就是80MB。
)

var calibrateCallsThreshold uint64 = 128 // 多少次调用后进行校准
var defaultPool = NewBFPool(128)

// var defaultPool = NewBBPool(128, 128*1024)

// GetWithCapacity returns a buffer with at least the specified capacity.
//
// Prefer this over Get when the expected size is known, to avoid
// reallocations on the first Write/Set/SetString.
func GetWithCapacity(capacity int) *ByteBuffer {
	return defaultPool.GetWithCap(capacity)
}

func GetWithLenCap(len int, capacity int) *ByteBuffer {
	bb := defaultPool.GetWithCap(capacity)
	bb.B = bb.B[:len]
	return bb
}

// Put returns byte buffer to the pool.
//
// ByteBuffer.B mustn't be touched after returning it to the pool.
// Otherwise data races will occur.
func Put(b *ByteBuffer) { defaultPool.Put(b) }

func NewByteBuffer(b []byte) *ByteBuffer {
	return &ByteBuffer{B: b}
}
