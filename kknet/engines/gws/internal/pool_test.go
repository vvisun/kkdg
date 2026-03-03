package internal

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBufferPool(t *testing.T) {
	var as = assert.New(t)
	var pool = NewBufferPool(128, 128*1024)

	pool.Put(bytes.NewBuffer(AlphabetNumeric.Generate(128)))
	for i := 0; i < 10; i++ {
		var n = AlphabetNumeric.Intn(126)
		var buf = pool.Get(n)
		as.Equal(128, buf.Cap())
		as.Equal(0, buf.Len())
	}
	for i := 0; i < 10; i++ {
		var buf = pool.Get(500)
		as.Equal(512, buf.Cap())
		as.Equal(0, buf.Len())
	}
	for i := 0; i < 10; i++ {
		var buf = pool.Get(2000)
		as.Equal(2048, buf.Cap())
		as.Equal(0, buf.Len())
	}
	for i := 0; i < 10; i++ {
		var buf = pool.Get(5000)
		as.Equal(8192, buf.Cap())
		as.Equal(0, buf.Len())
	}

	{
		pool.Put(bytes.NewBuffer(make([]byte, 2)))
		b := pool.Get(120)
		as.GreaterOrEqual(b.Cap(), 120)
	}
	{
		pool.Put(bytes.NewBuffer(make([]byte, 2000)))
		b := pool.Get(3000)
		as.GreaterOrEqual(b.Cap(), 3000)
	}

	pool.Put(nil)
	buffer := pool.Get(256 * 1024)
	as.GreaterOrEqual(buffer.Cap(), 256*1024)
}

func TestPool(t *testing.T) {
	var p = NewPool(func() int {
		return 0
	})
	assert.Equal(t, 0, p.Get())
	p.Put(1)
}

func TestPool_Get(t *testing.T) {
	var p = NewBufferPool(128, 1024*128)
	p.shards[128].Put(bytes.NewBuffer(AlphabetNumeric.Generate(120)))
	var buf = p.Get(128)
	assert.GreaterOrEqual(t, buf.Cap(), 128)
}

// TestBinaryCeil 测试 binaryCeil：向上取整到最近的 2 的幂
func TestBinaryCeil(t *testing.T) {
	tests := []struct {
		in   uint32
		want uint32
	}{
		{0, 0},
		{1, 1},
		{2, 2},
		{3, 4},
		{4, 4},
		{5, 8},
		{7, 8},
		{8, 8},
		{9, 16},
		{15, 16},
		{16, 16},
		{17, 32},
		{128, 128},
		{129, 256},
		{255, 256},
		{256, 256},
		{257, 512},
		{1024, 1024},
		{1025, 2048},
		{2048, 2048},
		{4096, 4096},
		{8192, 8192},
		{10000, 16384},
		{0x80000000, 0x80000000},
		{0x80000001, 0},
	}
	for _, tt := range tests {
		got := binaryCeil(tt.in)
		if got != tt.want {
			t.Errorf("binaryCeil(%d) = %d, want %d", tt.in, got, tt.want)
		}
	}
}

// ------------------------------------------------------------
// Benchmarks

func BenchmarkBufferPool_GetAndPut(b *testing.B) {
	pool := NewBufferPool(128, 128*1024)
	b.ReportAllocs()
	b.ResetTimer()
	wBytes := AlphabetNumeric.Generate(640)
	for i := 0; i < b.N; i++ {
		buf := pool.Get(1024)
		if _, err := buf.Write(wBytes); err != nil {
			b.Fatal(err)
		}
		pool.Put(buf)
	}
}

func BenchmarkBufferPool_Get_NoReuse(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	wBytes := AlphabetNumeric.Generate(640)
	for i := 0; i < b.N; i++ {
		// 模拟不使用池时的分配成本
		buf := bytes.NewBuffer(make([]byte, 0, 1024))
		if _, err := buf.Write(wBytes); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBufferPool_Get_Parallel(b *testing.B) {
	pool := NewBufferPool(128, 128*1024)
	b.ReportAllocs()
	b.ResetTimer()
	wBytes := AlphabetNumeric.Generate(128)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf := pool.Get(2048)
			if _, err := buf.Write(wBytes); err != nil {
				b.Fatal(err)
			}
			pool.Put(buf)
		}
	})
}
