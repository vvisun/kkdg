package kkbuffer

import (
	"testing"
)

// BenchmarkSet_Optimized tests the optimized Set method
func BenchmarkSet_Optimized(b *testing.B) {
	buf := Get()
	defer Put(buf)
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Set(data)
	}
}

// BenchmarkSet_Append tests the old append approach
func BenchmarkSet_Append(b *testing.B) {
	buf := Get()
	defer Put(buf)
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.B = append(buf.B[:0], data...)
	}
}

// BenchmarkSetString_Optimized tests the optimized SetString method
func BenchmarkSetString_Optimized(b *testing.B) {
	buf := Get()
	defer Put(buf)
	data := string(make([]byte, 1024))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.SetString(data)
	}
}

// BenchmarkSetString_Append tests the old append approach
func BenchmarkSetString_Append(b *testing.B) {
	buf := Get()
	defer Put(buf)
	data := string(make([]byte, 1024))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.B = append(buf.B[:0], data...)
	}
}

// BenchmarkSetWithCapacity tests SetWithCapacity method
func BenchmarkSetWithCapacity(b *testing.B) {
	buf := Get()
	defer Put(buf)
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.SetWithCapacity(data)
	}
}

// BenchmarkGrow tests Grow method
func BenchmarkGrow(b *testing.B) {
	buf := Get()
	defer Put(buf)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		buf.Grow(1024)
		buf.Write(make([]byte, 1024))
	}
}

// BenchmarkGetWithCapacity tests GetWithCapacity
func BenchmarkGetWithCapacity(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := GetWithCapacity(1024)
		buf.Write(make([]byte, 1024))
		Put(buf)
	}
}

// BenchmarkGetPut_WithCapacity tests Get/Put with capacity
func BenchmarkGetPut_WithCapacity(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := GetWithCapacity(1024)
		buf.WriteString("test")
		Put(buf)
	}
}

// BenchmarkGetPut_Standard tests standard Get/Put
func BenchmarkGetPut_Standard(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := Get()
		buf.WriteString("test")
		Put(buf)
	}
}
