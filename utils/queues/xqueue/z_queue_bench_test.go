package xqueue

import (
	"testing"

	"github.com/vvisun/kkdg/utils/queues/rbqueue"
)

func BenchmarkQueue_Push(b *testing.B) {
	q := NewQueue()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		q.Push(i)
	}
}

func BenchmarkQueue_Pop(b *testing.B) {
	q := NewQueue()
	for i := 0; i < b.N; i++ {
		q.Push(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Pop()
	}
}

func BenchmarkQueue_PushPop(b *testing.B) {
	q := NewQueue()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		q.Push(i)
		q.Pop()
	}
}

func BenchmarkQueue_Empty(b *testing.B) {
	q := NewQueue()
	for i := 0; i < 1000; i++ {
		q.Push(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Empty()
	}
}

func BenchmarkQueue_ConcurrentPush(b *testing.B) {
	q := NewQueue()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			q.Push(i)
			i++
		}
	})
}

func BenchmarkQueue_ConcurrentPushPop(b *testing.B) {
	q := NewQueue()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				q.Push(i)
			} else {
				q.Pop()
			}
			i++
		}
	})
}

func BenchmarkQueue_Large(b *testing.B) {
	q := NewQueue()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Push large batch
		for j := 0; j < 1000; j++ {
			q.Push(i*1000 + j)
		}
		// Pop large batch
		for j := 0; j < 1000; j++ {
			q.Pop()
		}
	}
}

func BenchmarkQueue_Stress(b *testing.B) {
	q := NewQueue()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Rapid push/pop
		for j := 0; j < 100; j++ {
			q.Push(i*100 + j)
		}
		for j := 0; j < 100; j++ {
			q.Pop()
		}
	}
}

// Compare rbqueue vs xqueue performance
func BenchmarkQueue_Compare_Push(b *testing.B) {
	b.Run("rbqueue", func(b *testing.B) {
		q := rbqueue.New(1000)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			q.Push(i)
		}
	})

	b.Run("xqueue", func(b *testing.B) {
		q := NewQueue()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			q.Push(i)
		}
	})
}
