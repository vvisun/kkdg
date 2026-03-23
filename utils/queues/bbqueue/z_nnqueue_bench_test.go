package bbqueue

import (
	"testing"

	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

const nnBenchBufferPoolSize = 10000

func nnBenchBufs(n int) []*kkbuffer.ByteBuffer {
	if n <= 0 {
		n = nnBenchBufferPoolSize
	}
	if n > nnBenchBufferPoolSize {
		n = nnBenchBufferPoolSize
	}
	bufs := make([]*kkbuffer.ByteBuffer, n)
	for i := 0; i < n; i++ {
		bufs[i] = kkbuffer.NewByteBuffer(make([]byte, 0, 64))
	}
	return bufs
}

func BenchmarkNNQueue_Push(b *testing.B) {
	bufs := nnBenchBufs(b.N)
	q := NewNNQueue(1000, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Push(bufs[i%len(bufs)])
	}
}

func BenchmarkNNQueue_Pop(b *testing.B) {
	bufs := nnBenchBufs(b.N)
	q := NewNNQueue(1000, false)
	for i := 0; i < b.N; i++ {
		q.Push(bufs[i%len(bufs)])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Pop()
	}
}

func BenchmarkNNQueue_PushPop(b *testing.B) {
	bufs := nnBenchBufs(b.N)
	q := NewNNQueue(1000, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Push(bufs[i%len(bufs)])
		q.Pop()
	}
}

func BenchmarkNNQueue_Len(b *testing.B) {
	bufs := nnBenchBufs(1000)
	q := NewNNQueue(1000, false)
	for i := 0; i < 1000; i++ {
		q.Push(bufs[i])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Len()
	}
}

func BenchmarkNNQueue_Push_WithPool(b *testing.B) {
	q := NewNNQueue(1000, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bb := kkbuffer.GetWithCapacity(128)
		q.Push(bb)
	}
	for q.Len() > 0 {
		kkbuffer.Put(q.Pop())
	}
}

func BenchmarkNNQueue_PushPop_WithPool(b *testing.B) {
	q := NewNNQueue(1000, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bb := kkbuffer.GetWithCapacity(128)
		q.Push(bb)
		got := q.Pop()
		if got != nil {
			kkbuffer.Put(got)
		}
	}
}

func BenchmarkNNQueue_PopMany(b *testing.B) {
	q := NewNNQueue(10000, false)
	recv := make([]*kkbuffer.ByteBuffer, 32)
	batchCnt := 8
	for j := 0; j < batchCnt*2; j++ {
		q.Push(kkbuffer.GetWithCapacity(128))
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if i%batchCnt == 0 {
			for j := 0; j < batchCnt; j++ {
				q.Push(kkbuffer.GetWithCapacity(128))
			}
		}
		_ = q.PopMany(batchCnt, recv, 0)
	}
}
