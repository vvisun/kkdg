package dqueue

import (
	"testing"

	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// Benchmark basic PushBack/PopFront throughput on Deque.
func BenchmarkDequePushPop(b *testing.B) {
	const batch = 1
	b.ReportAllocs()
	b.ResetTimer()
	d := New[*kkbuffer.ByteBuffer](batch)

	batchCnt := 32

	for n := 0; n < b.N; n++ {
		for i := 0; i < batch; i++ {
			for j := 0; j < batchCnt; j++ {
				d.PushBack(kkbuffer.GetWithCapacity(128))
			}
		}
		for i := 0; i < batch; i++ {
			for j := 0; j < batchCnt; j++ {
				kkbuffer.Put(d.PopFront())
			}
		}
	}
}

// Benchmark InsertAfter/Remove pattern to stress internal links update.
func BenchmarkDequeInsertRemove(b *testing.B) {
	const size = 1024
	d := New[int](size)
	head := d.PushBack(0)
	for i := 1; i < size; i++ {
		head = d.InsertAfter(i, head.Addr())
	}

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		// Remove all elements one by one from front and rebuild.
		for d.Len() > 0 {
			_ = d.PopFront()
		}
		for i := 0; i < size; i++ {
			d.PushBack(i)
		}
	}
}

// Benchmark MoveToFront/MoveToBack operations.
func BenchmarkDequeMove(b *testing.B) {
	const size = 1024
	d := New[int](size)
	addrs := make([]Pointer, 0, size)

	// Pre-fill
	for i := 0; i < size; i++ {
		ele := d.PushBack(i)
		addrs = append(addrs, ele.Addr())
	}

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		for _, addr := range addrs {
			d.MoveToFront(addr)
		}
		for _, addr := range addrs {
			d.MoveToBack(addr)
		}
	}
}
