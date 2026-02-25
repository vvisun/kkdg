package bbqueue

import (
	"testing"

	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

const benchBufferPoolSize = 100000

func benchBufs(n int) []*kkbuffer.ByteBuffer {
	if n <= 0 {
		n = benchBufferPoolSize
	}
	if n > benchBufferPoolSize {
		n = benchBufferPoolSize
	}
	bufs := make([]*kkbuffer.ByteBuffer, n)
	for i := 0; i < n; i++ {
		bufs[i] = &kkbuffer.ByteBuffer{B: make([]byte, 0, 64)}
	}
	return bufs
}

func BenchmarkBBQueue_Push(b *testing.B) {
	bufs := benchBufs(b.N)
	q := NewBBQueue[*kkbuffer.ByteBuffer](1000, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Push(bufs[i%len(bufs)])
	}
}

func BenchmarkBBQueue_Pop(b *testing.B) {
	bufs := benchBufs(b.N)
	q := NewBBQueue[*kkbuffer.ByteBuffer](1000, false)
	for i := 0; i < b.N; i++ {
		q.Push(bufs[i%len(bufs)])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Pop()
	}
}

func BenchmarkBBQueue_PushPop(b *testing.B) {
	bufs := benchBufs(b.N)
	q := NewBBQueue[*kkbuffer.ByteBuffer](1000, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Push(bufs[i%len(bufs)])
		q.Pop()
	}
}

func BenchmarkBBQueue_Len(b *testing.B) {
	bufs := benchBufs(1000)
	q := NewBBQueue[*kkbuffer.ByteBuffer](1000, false)
	for i := 0; i < 1000; i++ {
		q.Push(bufs[i])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Len()
	}
}

func BenchmarkBBQueue_Grow(b *testing.B) {
	bufs := benchBufs(b.N)
	q := NewBBQueue[*kkbuffer.ByteBuffer](4, false) // 小初始容量以触发多次 grow
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Push(bufs[i%len(bufs)])
	}
}

func BenchmarkBBQueue_Push_WithPool(b *testing.B) {
	q := NewBBQueue[*kkbuffer.ByteBuffer](1000, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bb := kkbuffer.Get()
		q.Push(bb)
	}
	// 清空并归还，避免池膨胀影响后续用例（本 benchmark 不包含 Pop）
	for q.Len() > 0 {
		kkbuffer.Put(q.Pop())
	}
}

func BenchmarkBBQueue_PushPop_WithPool(b *testing.B) {
	q := NewBBQueue[*kkbuffer.ByteBuffer](1000, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bb := kkbuffer.Get()
		q.Push(bb)
		got := q.Pop()
		if got != nil {
			kkbuffer.Put(got)
		}
	}
}

func BenchmarkBBQueue_PopMany(b *testing.B) {
	bufs := nnBenchBufs(10000)
	q := NewNNQueue[*kkbuffer.ByteBuffer](10000, false)
	for i := 0; i < 10000; i++ {
		q.Push(bufs[i])
	}
	recv := make([]*kkbuffer.ByteBuffer, 32)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n := q.PopMany(32, recv, 0)
		for j := 0; j < n; j++ {
			q.Push(recv[j])
			recv[j] = nil
		}
	}
}
