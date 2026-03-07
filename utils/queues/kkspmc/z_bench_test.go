package kkspmc

import (
	"sync"
	"testing"
)

func BenchmarkQueue_SPMC_PushPop(b *testing.B) {
	q := NewQueue[int]()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		val := i
		q.Push(&val)
		_, _ = q.Pop()
	}
}

func BenchmarkQueue_SPMC_SingleProducerMultipleConsumers(b *testing.B) {
	q := NewQueue[int]()
	done := make(chan struct{})
	const consumers = 4
	var wg sync.WaitGroup
	wg.Add(consumers)
	for c := 0; c < consumers; c++ {
		go func() {
			defer wg.Done()
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
	for i := 0; i < b.N; i++ {
		val := i
		q.Push(&val)
	}
	close(done)
	wg.Wait()
}
