package kkmpmc

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestQueue_PushPop(t *testing.T) {
	q := NewQueue[int]()
	if !q.IsEmpty() {
		t.Error("new queue should be empty")
	}

	one := 1
	q.Push(&one)
	if q.IsEmpty() {
		t.Error("queue should not be empty after Push")
	}
	if q.Len() != 1 {
		t.Errorf("Len() = %d, want 1", q.Len())
	}

	v, ok := q.Pop()
	if !ok || v == nil || *v != 1 {
		t.Fatalf("Pop() = %v, %v, want &1, true", v, ok)
	}
	if !q.IsEmpty() {
		t.Error("queue should be empty after Pop")
	}

	_, ok = q.Pop()
	if ok {
		t.Error("Pop on empty queue should return false")
	}
}

func TestQueue_Unbounded(t *testing.T) {
	q := NewQueue[int]()
	const N = 10000
	for i := 0; i < N; i++ {
		val := i
		q.Push(&val)
	}
	if q.Len() != N {
		t.Errorf("Len() = %d, want %d", q.Len(), N)
	}
	for i := 0; i < N; i++ {
		v, ok := q.Pop()
		if !ok || v == nil || *v != i {
			t.Fatalf("Pop %d: got %v, %v", i, v, ok)
		}
	}
	if !q.IsEmpty() {
		t.Error("queue should be empty after Pop all")
	}
}

func TestQueue_MPMC_Concurrent(t *testing.T) {
	q := NewQueue[int]()
	const producers = 4
	const consumers = 4
	const perProducer = 2000
	total := producers * perProducer

	var wg sync.WaitGroup
	wg.Add(producers)
	for p := 0; p < producers; p++ {
		p := p
		go func() {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				val := new(int)
				*val = p*perProducer + i
				q.Push(val)
			}
		}()
	}
	wg.Wait()

	if q.Len() != total {
		t.Errorf("Len() = %d, want %d", q.Len(), total)
	}

	var popped atomic.Int64
	wg.Add(consumers)
	for c := 0; c < consumers; c++ {
		go func() {
			defer wg.Done()
			for popped.Load() < int64(total) {
				if _, ok := q.Pop(); ok {
					popped.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	if popped.Load() != int64(total) {
		t.Errorf("popped = %d, want %d", popped.Load(), total)
	}
}

func TestQueue_MPMC_ProduceAndConsume(t *testing.T) {
	q := NewQueue[int]()
	const total = 8000
	const producers = 4
	const consumers = 4
	var popped atomic.Int64
	var wg sync.WaitGroup

	wg.Add(consumers)
	for c := 0; c < consumers; c++ {
		go func() {
			defer wg.Done()
			for popped.Load() < total {
				if _, ok := q.Pop(); ok {
					popped.Add(1)
				}
			}
		}()
	}

	var id atomic.Int64
	wg.Add(producers)
	for p := 0; p < producers; p++ {
		go func() {
			defer wg.Done()
			for i := 0; i < total/producers; i++ {
				val := new(int)
				*val = int(id.Add(1) - 1)
				q.Push(val)
			}
		}()
	}
	wg.Wait()

	for popped.Load() < total {
		// spin until consumers drain
	}
	if popped.Load() != total {
		t.Errorf("popped = %d, want %d", popped.Load(), total)
	}
}
