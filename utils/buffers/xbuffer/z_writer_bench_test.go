package xbuffer

import (
	"encoding/binary"
	"testing"
)

var sinkWriter *Writer

// BenchmarkNewWriter 创建Writer性能测试
func BenchmarkNewWriter(b *testing.B) {
	buf := make([]byte, 100)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sinkWriter = NewWriter(buf)
	}
}

// BenchmarkNewWriterWithCapacity 创建指定容量的Writer性能测试
func BenchmarkNewWriterWithCapacity(b *testing.B) {
	cap := 100

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sinkWriter = NewWriterWithCapacity(cap)
	}
}

// BenchmarkWriter_Write 写入字节性能测试
func BenchmarkWriter_Write(b *testing.B) {
	writer := NewWriterWithCapacity(1000)
	data := []byte("hello world")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		writer.off = 0
		_, _ = writer.Write(data)
	}
}

// BenchmarkWriter_WriteString 写入字符串性能测试
func BenchmarkWriter_WriteString(b *testing.B) {
	writer := NewWriterWithCapacity(1000)
	str := "hello world"

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		writer.off = 0
		writer.WriteString(str)
	}
}

// BenchmarkWriter_WriteInt32s 写入int32性能测试
func BenchmarkWriter_WriteInt32s(b *testing.B) {
	writer := NewWriterWithCapacity(1000)
	values := []int32{1, 2, 3, 4, 5}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		writer.off = 0
		writer.WriteInt32s(binary.BigEndian, values...)
	}
}

// BenchmarkWriter_WriteUint32s 写入uint32性能测试
func BenchmarkWriter_WriteUint32s(b *testing.B) {
	writer := NewWriterWithCapacity(1000)
	values := []uint32{1, 2, 3, 4, 5}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		writer.off = 0
		writer.WriteUint32s(binary.BigEndian, values...)
	}
}

// BenchmarkWriter_WriteInt64s 写入int64性能测试
func BenchmarkWriter_WriteInt64s(b *testing.B) {
	writer := NewWriterWithCapacity(1000)
	values := []int64{1, 2, 3, 4, 5}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		writer.off = 0
		writer.WriteInt64s(binary.BigEndian, values...)
	}
}

// BenchmarkWriter_Grow 扩容性能测试
func BenchmarkWriter_Grow(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		writer := NewWriterWithCapacity(10)
		writer.Grow(100)
	}
}

// BenchmarkWriter_Bytes 获取字节数据性能测试
func BenchmarkWriter_Bytes(b *testing.B) {
	writer := NewWriterWithCapacity(100)
	writer.WriteString("hello world")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sinkByteSlice = writer.Bytes()
	}
}

// BenchmarkWriterPool_Get 从池获取Writer性能测试
func BenchmarkWriterPool_Get(b *testing.B) {
	pool := NewWriterPool(10)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sinkWriter = pool.Get(100)
	}
}

// BenchmarkWriterPool_Get_Put 从池获取和放回性能测试
func BenchmarkWriterPool_Get_Put(b *testing.B) {
	pool := NewWriterPool(10)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		writer := pool.Get(100)
		writer.Release()
		pool.Put(writer)
	}
}

// BenchmarkMallocWriter 从默认池分配性能测试
func BenchmarkMallocWriter(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sinkWriter = MallocWriter(100)
	}
}

// BenchmarkWriterPool_Concurrent 并发获取性能测试
func BenchmarkWriterPool_Concurrent(b *testing.B) {
	pool := NewWriterPool(10)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			writer := pool.Get(100)
			writer.Release()
			pool.Put(writer)
		}
	})
}
