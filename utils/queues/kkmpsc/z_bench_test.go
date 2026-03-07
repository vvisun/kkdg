package kkmpsc

import (
	"sync"
	"testing"
)

func BenchmarkQueue_MPSC_PushPop(b *testing.B) {
	q := NewQueue[int]()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		val := i
		q.Push(&val)
		_, _ = q.Pop()
	}
}

func BenchmarkQueue_MPSC_MultiProducerSingleConsumer(b *testing.B) {
	q := NewQueue[int]()
	done := make(chan struct{})
	//单消费者
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
	const producers = 8
	var wg sync.WaitGroup
	wg.Add(producers)
	//多生产者
	for p := 0; p < producers; p++ {
		go func() {
			defer wg.Done()
			for i := 0; i < b.N/producers; i++ {
				val := i
				q.Push(&val)
			}
		}()
	}
	wg.Wait()
	close(done)
}
