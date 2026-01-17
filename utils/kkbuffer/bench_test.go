package kkbuffer

import "testing"

func BenchmarkPool_GetPut(b *testing.B) {
	pool := &Pool{}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf := pool.Get()
		buf.WriteString("test")
		pool.Put(buf)
	}
}

func BenchmarkGetPut(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf := Get()
		buf.WriteString("test")
		Put(buf)
	}
}

func BenchmarkByteBuffer_Write(b *testing.B) {
	buf := &ByteBuffer{}
	data := []byte("test data")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf.Write(data)
	}
}

func BenchmarkByteBuffer_WriteString(b *testing.B) {
	buf := &ByteBuffer{}
	data := "test data"
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf.WriteString(data)
	}
}
