// Package kkbuffer 提供字节缓冲池与 ByteBuffer，供网络封包等场景复用。
// Get/GetWithCapacity 从池中获取，Put 归还；归还后 ByteBuffer.B 不可再访问。
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

// 从池中获取一个字节缓冲区，bb.B长度为0，容量为capacity。
// 注意：ByteBuffer在哪里终止使用，就在哪里用kkbuffer.Put()释放。
//  @param capacity 期望的bb.B的容量。
//  @return *ByteBuffer 字节缓冲区 bb.B长度为0，容量为capacity。
func GetWithCapacity(capacity int) *ByteBuffer {
	return defaultPool.GetWithCap(capacity)
}

// 从池中获取一个字节缓冲区，bb.B长度为len，容量为capacity。
// 注意：ByteBuffer在哪里终止使用，就在哪里用kkbuffer.Put()释放。
//  @param len 期望的bb.B的长度。
//  @param capacity 期望的bb.B的容量。
//  @return *ByteBuffer 字节缓冲区 bb.B长度为len，容量为capacity。
func GetWithLenCap(len int, capacity int) *ByteBuffer {
	bb := defaultPool.GetWithCap(capacity)
	bb.B = bb.B[:len]
	return bb
}

// 将字节缓冲区放回池中。
// 注意：ByteBuffer在哪里终止使用，就在哪里用kkbuffer.Put()释放。
//  @note ByteBuffer.B mustn't be touched after returning it to the pool. Otherwise data races will occur.
//  @param bb 字节缓冲区
func Put(bb *ByteBuffer) { defaultPool.Put(bb) }

// 创建一个字节缓冲区，bb.B为b。
// 注意：ByteBuffer在哪里终止使用，就在哪里用kkbuffer.Put()释放。
//  @param b 字节缓冲区
//  @return *ByteBuffer 字节缓冲区 bb.B为b。
func NewByteBuffer(b []byte) *ByteBuffer {
	return &ByteBuffer{B: b}
}
