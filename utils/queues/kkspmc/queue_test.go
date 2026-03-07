package kkspmc

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

func TestQueue_SPMC_Concurrent(t *testing.T) {
	q := NewQueue[int]()
	const N = 10000
	for i := 0; i < N; i++ {
		val := new(int)
		*val = i
		q.Push(val)
	}

	const consumers = 8
	var wg sync.WaitGroup
	var popped atomic.Int64
	wg.Add(consumers)
	for c := 0; c < consumers; c++ {
		go func() {
			defer wg.Done()
			for {
				_, ok := q.Pop()
				if !ok {
					return
				}
				popped.Add(1)
			}
		}()
	}
	wg.Wait()

	if popped.Load() != N {
		t.Errorf("popped = %d, want %d", popped.Load(), N)
	}
	if !q.IsEmpty() {
		t.Error("queue should be empty after all consumers done")
	}
}

func TestQueue_SPMC_SingleProducerMultipleConsumers(t *testing.T) {
	q := NewQueue[int]()
	const total = 8000
	const consumers = 4
	var popped atomic.Int64
	var wg sync.WaitGroup
	wg.Add(consumers)
	for c := 0; c < consumers; c++ {
		go func() {
			defer wg.Done()
			for {
				if _, ok := q.Pop(); ok {
					popped.Add(1)
				}
				if popped.Load() >= total {
					return
				}
			}
		}()
	}
	for i := 0; i < total; i++ {
		val := new(int)
		*val = i
		q.Push(val)
	}
	wg.Wait()
	if popped.Load() != total {
		t.Errorf("popped = %d, want %d", popped.Load(), total)
	}
}
