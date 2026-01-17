package rbqueue

import (
	"testing"
)

func TestQueue_New(t *testing.T) {
	q := New(10)
	if q == nil {
		t.Fatal("New(10) returned nil")
	}
	if q.Length() != 0 {
		t.Errorf("New queue length = %d, want 0", q.Length())
	}
	if !q.Empty() {
		t.Error("New queue should be empty")
	}
}

func TestQueue_PushPop(t *testing.T) {
	q := New(4)

	// Push single item
	q.Push(1)
	if q.Length() != 1 {
		t.Errorf("Length() = %d, want 1", q.Length())
	}
	if q.Empty() {
		t.Error("Queue should not be empty after Push")
	}

	// Pop item
	val, ok := q.Pop()
	if !ok {
		t.Error("Pop() should return true")
	}
	if val != 1 {
		t.Errorf("Pop() = %v, want 1", val)
	}
	if q.Length() != 0 {
		t.Errorf("Length() = %d, want 0", q.Length())
	}
	if !q.Empty() {
		t.Error("Queue should be empty after Pop")
	}
}

func TestQueue_PushPopMultiple(t *testing.T) {
	q := New(4)

	// Push multiple items
	for i := 0; i < 10; i++ {
		q.Push(i)
	}

	if q.Length() != 10 {
		t.Errorf("Length() = %d, want 10", q.Length())
	}

	// Pop all items
	for i := 0; i < 10; i++ {
		val, ok := q.Pop()
		if !ok {
			t.Errorf("Pop() failed at iteration %d", i)
		}
		if val != i {
			t.Errorf("Pop() = %v, want %d", val, i)
		}
	}

	if !q.Empty() {
		t.Error("Queue should be empty after popping all items")
	}
}

func TestQueue_PopEmpty(t *testing.T) {
	q := New(4)

	val, ok := q.Pop()
	if ok {
		t.Errorf("Pop() on empty queue returned ok=true, want false")
	}
	if val != nil {
		t.Errorf("Pop() on empty queue = %v, want nil", val)
	}
}

func TestQueue_PopMany(t *testing.T) {
	q := New(4)

	// Push items
	for i := 0; i < 5; i++ {
		q.Push(i)
	}

	// Pop many
	items, ok := q.PopMany(3)
	if !ok {
		t.Error("PopMany(3) should return true")
	}
	if len(items) != 3 {
		t.Errorf("PopMany(3) returned %d items, want 3", len(items))
	}
	for i, val := range items {
		if val != i {
			t.Errorf("PopMany()[%d] = %v, want %d", i, val, i)
		}
	}

	if q.Length() != 2 {
		t.Errorf("Length() = %d, want 2", q.Length())
	}
}

func TestQueue_PopManyAll(t *testing.T) {
	q := New(4)

	// Push items
	for i := 0; i < 5; i++ {
		q.Push(i)
	}

	// Pop more than available
	items, ok := q.PopMany(10)
	if !ok {
		t.Error("PopMany(10) should return true")
	}
	if len(items) != 5 {
		t.Errorf("PopMany(10) returned %d items, want 5", len(items))
	}

	if !q.Empty() {
		t.Error("Queue should be empty after PopMany all items")
	}
}

func TestQueue_PopManyEmpty(t *testing.T) {
	q := New(4)

	items, ok := q.PopMany(5)
	if ok {
		t.Error("PopMany() on empty queue should return false")
	}
	if items != nil {
		t.Errorf("PopMany() on empty queue = %v, want nil", items)
	}
}

func TestQueue_Resize(t *testing.T) {
	q := New(4)

	// Fill queue to trigger resize
	for i := 0; i < 10; i++ {
		q.Push(i)
	}

	if q.Length() != 10 {
		t.Errorf("Length() = %d, want 10", q.Length())
	}

	// Pop all and verify order
	for i := 0; i < 10; i++ {
		val, ok := q.Pop()
		if !ok {
			t.Fatalf("Pop() failed at iteration %d", i)
		}
		if val != i {
			t.Errorf("Pop() = %v, want %d", val, i)
		}
	}
}

func TestQueue_ConcurrentPush(t *testing.T) {
	q := New(100)
	done := make(chan bool, 10)

	// Concurrent push
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				q.Push(id*100 + j)
			}
			done <- true
		}(i)
	}

	// Wait for all pushes
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify total length
	if q.Length() != 1000 {
		t.Errorf("Length() = %d, want 1000", q.Length())
	}
}

func TestQueue_LengthConsistency(t *testing.T) {
	q := New(4)

	// Push and verify length
	for i := 0; i < 20; i++ {
		q.Push(i)
		if q.Length() != int64(i+1) {
			t.Errorf("After Push(%d), Length() = %d, want %d", i, q.Length(), i+1)
		}
	}

	// Pop and verify length
	for i := 0; i < 20; i++ {
		q.Pop()
		if q.Length() != int64(19-i) {
			t.Errorf("After Pop(), Length() = %d, want %d", q.Length(), 19-i)
		}
	}
}

func TestQueue_EmptyAfterPopAll(t *testing.T) {
	q := New(4)

	// Push and pop
	for i := 0; i < 5; i++ {
		q.Push(i)
	}

	for i := 0; i < 5; i++ {
		q.Pop()
	}

	if !q.Empty() {
		t.Error("Queue should be empty after popping all items")
	}
	if q.Length() != 0 {
		t.Errorf("Length() = %d, want 0", q.Length())
	}
}

func TestQueue_ZeroInitialSize(t *testing.T) {
	// New(0) will cause divide by zero in Push, so we test it panics
	defer func() {
		if r := recover(); r == nil {
			t.Error("New(0) should panic or handle gracefully")
		}
	}()

	q := New(0)
	if q == nil {
		t.Fatal("New(0) returned nil")
	}

	// This will panic due to mod being 0
	q.Push(1)
}

func TestQueue_NegativeInitialSize(t *testing.T) {
	// Negative size should be handled or panic
	// We test it doesn't crash in New, but behavior is undefined
	defer func() {
		if r := recover(); r != nil {
			// Panic is acceptable for invalid input
			return
		}
	}()

	q := New(-1)
	if q == nil {
		t.Fatal("New(-1) returned nil")
	}

	// Behavior is undefined, but shouldn't crash
	// We don't test Push as it may have undefined behavior
}
