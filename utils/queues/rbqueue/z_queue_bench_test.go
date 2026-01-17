package rbqueue

import (
	"testing"
)

func BenchmarkQueue_Push(b *testing.B) {
	q := New(1000)
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		q.Push(i)
	}
}

func BenchmarkQueue_Pop(b *testing.B) {
	q := New(1000)
	for i := 0; i < b.N; i++ {
		q.Push(i)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Pop()
	}
}

func BenchmarkQueue_PushPop(b *testing.B) {
	q := New(1000)
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		q.Push(i)
		q.Pop()
	}
}

func BenchmarkQueue_PushPopMany(b *testing.B) {
	q := New(1000)
	batchSize := int64(100)
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		// Push batch
		for j := int64(0); j < batchSize; j++ {
			q.Push(i*int(batchSize) + int(j))
		}
		// Pop batch
		q.PopMany(batchSize)
	}
}

func BenchmarkQueue_Length(b *testing.B) {
	q := New(1000)
	for i := 0; i < 1000; i++ {
		q.Push(i)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Length()
	}
}

func BenchmarkQueue_Empty(b *testing.B) {
	q := New(1000)
	for i := 0; i < 1000; i++ {
		q.Push(i)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Empty()
	}
}

func BenchmarkQueue_ConcurrentPush(b *testing.B) {
	q := New(10000)
	b.ResetTimer()
	
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			q.Push(i)
			i++
		}
	})
}

func BenchmarkQueue_Resize(b *testing.B) {
	q := New(4) // Small initial size to trigger resizes
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		q.Push(i)
	}
}

func BenchmarkQueue_PopMany_Small(b *testing.B) {
	q := New(1000)
	for i := 0; i < 10000; i++ {
		q.Push(i)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.PopMany(10)
	}
}

func BenchmarkQueue_PopMany_Large(b *testing.B) {
	q := New(1000)
	for i := 0; i < 10000; i++ {
		q.Push(i)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.PopMany(100)
	}
}
