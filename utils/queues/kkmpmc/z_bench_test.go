package kkmpmc

import (
	"sync"
	"testing"
)

func BenchmarkQueue_MPMC_PushPop(b *testing.B) {
	q := NewQueue[int]()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		val := i
		q.Push(&val)
		_, _ = q.Pop()
	}
}

func BenchmarkQueue_MPMC_MultiProducerMultiConsumer(b *testing.B) {
	q := NewQueue[int]()
	done := make(chan struct{})
	const producers = 4
	const consumers = 4
	var consWg, prodWg sync.WaitGroup
	consWg.Add(consumers)
	for c := 0; c < consumers; c++ {
		go func() {
			defer consWg.Done()
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
	}
	b.ResetTimer()
	prodWg.Add(producers)
	for p := 0; p < producers; p++ {
		go func() {
			defer prodWg.Done()
			for i := 0; i < b.N/producers; i++ {
				val := i
				q.Push(&val)
			}
		}()
	}
	prodWg.Wait()
	close(done)
	consWg.Wait()
}
