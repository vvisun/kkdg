package kkspsc

import (
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

func TestQueue_SPSC_Sequential(t *testing.T) {
	q := NewQueue[int]()
	const N = 1000
	for i := 0; i < N; i++ {
		val := i
		q.Push(&val)
	}
	for i := 0; i < N; i++ {
		v, ok := q.Pop()
		if !ok || v == nil || *v != i {
			t.Fatalf("Pop %d: got %v, %v", i, v, ok)
		}
	}
}

func TestQueue_SPSC_Concurrent(t *testing.T) {
	q := NewQueue[int]()
	const N = 100000
	var produced, consumed atomic.Int64

	go func() {
		for i := 0; i < N; i++ {
			val := new(int)
			*val = i
			q.Push(val)
			produced.Add(1)
		}
	}()

	for i := 0; i < N; i++ {
		var v *int
		for {
			var ok bool
			v, ok = q.Pop()
			if ok {
				break
			}
		}
		if v != nil && *v != i {
			t.Errorf("at %d: got %d", i, *v)
		}
		consumed.Add(1)
	}

	if produced.Load() != N || consumed.Load() != N {
		t.Errorf("produced=%d consumed=%d want %d", produced.Load(), consumed.Load(), N)
	}
}
