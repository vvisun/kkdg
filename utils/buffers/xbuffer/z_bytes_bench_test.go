package xbuffer

import (
	"testing"
)

var sinkBytes *Bytes

// BenchmarkNewBytes 创建Bytes性能测试
func BenchmarkNewBytes(b *testing.B) {
	data := make([]byte, 100)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sinkBytes = NewBytes(data)
	}
}

// BenchmarkNewBytesWithCapacity 创建指定容量的Bytes性能测试
func BenchmarkNewBytesWithCapacity(b *testing.B) {
	cap := 100

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sinkBytes = NewBytesWithCapacity(cap)
	}
}

// BenchmarkBytes_Len 获取长度性能测试
func BenchmarkBytes_Len(b *testing.B) {
	bytes := NewBytesWithCapacity(100)

	b.ResetTimer()
	b.ReportAllocs()

	var result int
	for i := 0; i < b.N; i++ {
		result = bytes.Len()
	}
	_ = result
}

// BenchmarkBytes_Cap 获取容量性能测试
func BenchmarkBytes_Cap(b *testing.B) {
	bytes := NewBytesWithCapacity(100)

	b.ResetTimer()
	b.ReportAllocs()

	var result int
	for i := 0; i < b.N; i++ {
		result = bytes.Cap()
	}
	_ = result
}

// BenchmarkBytes_Bytes 获取字节数据性能测试
func BenchmarkBytes_Bytes(b *testing.B) {
	bytes := NewBytes([]byte("hello world"))

	b.ResetTimer()
	b.ReportAllocs()

	var result []byte
	for i := 0; i < b.N; i++ {
		result = bytes.Bytes()
	}
	_ = result
}

// BenchmarkBytesPool_Get 从池获取Bytes性能测试
func BenchmarkBytesPool_Get(b *testing.B) {
	pool := NewBytesPool(10)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sinkBytes = pool.Get(100)
	}
}

// BenchmarkBytesPool_Get_Put 从池获取和放回性能测试
func BenchmarkBytesPool_Get_Put(b *testing.B) {
	pool := NewBytesPool(10)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		bytes := pool.Get(100)
		pool.Put(bytes)
	}
}

// BenchmarkMallocBytes 从默认池分配性能测试
func BenchmarkMallocBytes(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sinkBytes = MallocBytes(100)
	}
}

// BenchmarkBytesPool_Concurrent 并发获取性能测试
func BenchmarkBytesPool_Concurrent(b *testing.B) {
	pool := NewBytesPool(10)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bytes := pool.Get(100)
			pool.Put(bytes)
		}
	})
}
