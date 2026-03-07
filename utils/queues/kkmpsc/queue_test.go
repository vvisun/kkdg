package kkmpsc

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

func TestQueue_MPSC_Concurrent(t *testing.T) {
	q := NewQueue[int]()
	const producers = 8
	const perProducer = 2000
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

	if q.Len() != producers*perProducer {
		t.Errorf("Len() = %d, want %d", q.Len(), producers*perProducer)
	}

	seen := make(map[int]bool)
	for i := 0; i < producers*perProducer; i++ {
		v, ok := q.Pop()
		if !ok || v == nil {
			t.Fatalf("Pop %d: got nil or !ok", i)
		}
		if seen[*v] {
			t.Errorf("duplicate value %d", *v)
		}
		seen[*v] = true
	}
	if !q.IsEmpty() {
		t.Error("queue should be empty after Pop all")
	}
}

func TestQueue_MPSC_ProduceAndConsume(t *testing.T) {
	q := NewQueue[int]()
	const total = 10000
	var consumed atomic.Int64
	var id atomic.Int64

	go func() {
		for consumed.Load() < total {
			if _, ok := q.Pop(); ok {
				consumed.Add(1)
			}
		}
	}()

	const producers = 4
	var wg sync.WaitGroup
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

	for consumed.Load() < total {
		// spin until consumer drains
	}
	if q.Len() != 0 {
		t.Errorf("Len() = %d, want 0", q.Len())
	}
}
