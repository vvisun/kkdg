package kkbuffer

import (
	"encoding/binary"
	"strings"
	"sync"
	"testing"
)

func BenchmarkGetPut(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf := GetWithCapacity(128)
		buf.SetString("test")
		Put(buf)
	}
}

func BenchmarkPool_GetPut(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf := GetWithCapacity(128)
		buf.SetString("test")
		Put(buf)
	}
}

func BenchmarkByteBuffer_WriteString(b *testing.B) {
	buf := GetWithCapacity(128)
	defer Put(buf)
	data := "test data"
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf.SetString(data)
	}
}

func BenchmarkSet(b *testing.B) {
	buf := GetWithCapacity(128)
	defer Put(buf)
	buf.grow(1024)
	buf.Reset()
	data := make([]byte, 512)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf.SetBytes(data)
	}
}

func BenchmarkSet_Append(b *testing.B) {
	buf := GetWithCapacity(128)
	defer Put(buf)
	buf.grow(1024)
	buf.Reset()
	data := make([]byte, 512)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf.B = append(buf.B[:0], data...)
	}
}

func BenchmarkSetString(b *testing.B) {
	buf := GetWithCapacity(128)
	defer Put(buf)
	buf.grow(1024)
	buf.Reset()
	data := string(make([]byte, 512))

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf.SetString(data)
	}
}

func BenchmarkGetPut_WithSet(b *testing.B) {
	data := make([]byte, 256)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf := GetWithCapacity(128)
		buf.SetBytes(data)
		Put(buf)
	}
}

func BenchmarkGetPut_WithSetString(b *testing.B) {
	data := strings.Repeat("x", 256)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf := GetWithCapacity(128)
		buf.SetString(data)
		Put(buf)
	}
}

// 并发性能测试
func BenchmarkGetPut_Concurrent(b *testing.B) {
	b.ReportAllocs()
	wg := sync.WaitGroup{}
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := GetWithCapacity(128)
			buf.SetString("test")
			Put(buf)
		}()
	}
	wg.Wait()
}

func Benchmark_AA(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	data := []byte("test data")
	for i := 0; i < b.N; i++ {
		bb := GetWithCapacity(len(data) + 4)
		bb.B = bb.B[:0]
		bb.B = append(bb.B, 0, 0, 0, 0)
		binary.BigEndian.PutUint32(bb.B[:4], uint32(len(data)))
		bb.B = append(bb.B, data...)
		Put(bb)
	}
}

func Benchmark_BB(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	data := []byte("test data")
	for i := 0; i < b.N; i++ {
		bb := GetWithCapacity(len(data) + 4)
		bb.B = bb.B[:len(data)+4]
		binary.BigEndian.PutUint32(bb.B[:4], uint32(len(data)))
		copy(bb.B[4:], data)
		Put(bb)
	}
}
