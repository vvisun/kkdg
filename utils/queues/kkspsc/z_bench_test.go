package kkspsc

import (
	"testing"
)

func BenchmarkQueue_SPSC_PushPop(b *testing.B) {
	q := NewQueue[int]()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		val := i
		q.Push(&val)
		_, _ = q.Pop()
	}
}

func BenchmarkQueue_SPSC_ProducerConsumer(b *testing.B) {
	q := NewQueue[int]()
	done := make(chan struct{})
	go func() {
		for {
			if _, ok := q.Pop(); !ok {
				select {
				case <-done:
					return
				default:
				}
				continue
			}
		}
	}()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		val := i
		q.Push(&val)
	}
	close(done)
}
