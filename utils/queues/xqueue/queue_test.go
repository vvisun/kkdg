package xqueue

import (
	"testing"
)

func TestQueue_New(t *testing.T) {
	q := NewQueue()
	if q.head == nil {
		t.Fatal("NewQueue() head should not be nil")
	}
	if q.tail == nil {
		t.Fatal("NewQueue() tail should not be nil")
	}
	if !q.Empty() {
		t.Error("New queue should be empty")
	}
}

func TestQueue_PushPop(t *testing.T) {
	q := NewQueue()
	
	// Push single item
	q.Push(1)
	if q.Empty() {
		t.Error("Queue should not be empty after Push")
	}
	
	// Pop item
	val := q.Pop()
	if val != 1 {
		t.Errorf("Pop() = %v, want 1", val)
	}
	if val == nil {
		t.Error("Pop() should not return nil")
	}
	
	if !q.Empty() {
		t.Error("Queue should be empty after Pop")
	}
}

func TestQueue_PushPopMultiple(t *testing.T) {
	q := NewQueue()
	
	// Push multiple items
	for i := 0; i < 10; i++ {
		q.Push(i)
	}
	
	// Pop all items
	for i := 0; i < 10; i++ {
		val := q.Pop()
		if val != i {
			t.Errorf("Pop() = %v, want %d", val, i)
		}
	}
	
	if !q.Empty() {
		t.Error("Queue should be empty after popping all items")
	}
}

func TestQueue_PopEmpty(t *testing.T) {
	q := NewQueue()
	
	val := q.Pop()
	if val != nil {
		t.Errorf("Pop() on empty queue = %v, want nil", val)
	}
}

func TestQueue_Empty(t *testing.T) {
	q := NewQueue()
	
	if !q.Empty() {
		t.Error("New queue should be empty")
	}
	
	q.Push(1)
	if q.Empty() {
		t.Error("Queue should not be empty after Push")
	}
	
	q.Pop()
	if !q.Empty() {
		t.Error("Queue should be empty after Pop")
	}
}

func TestQueue_Order(t *testing.T) {
	q := NewQueue()
	
	// Push in order
	for i := 0; i < 100; i++ {
		q.Push(i)
	}
	
	// Pop and verify FIFO order
	for i := 0; i < 100; i++ {
		val := q.Pop()
		if val != i {
			t.Errorf("Pop() = %v, want %d (FIFO order)", val, i)
		}
	}
}

func TestQueue_ConcurrentPush(t *testing.T) {
	q := NewQueue()
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
	
	// Pop all items and verify we got all 1000 items
	items := make(map[int]bool)
	count := 0
	for !q.Empty() {
		val := q.Pop()
		if val != nil {
			items[val.(int)] = true
			count++
		}
	}
	
	if count != 1000 {
		t.Errorf("Expected 1000 items, got %d", count)
	}
	if len(items) != 1000 {
		t.Errorf("Expected 1000 unique items, got %d", len(items))
	}
}

func TestQueue_MixedTypes(t *testing.T) {
	q := NewQueue()
	
	// Push different types
	q.Push(1)
	q.Push("hello")
	q.Push(3.14)
	q.Push(true)
	q.Push([]int{1, 2, 3})
	
	// Pop and verify types
	val1 := q.Pop()
	if val1 != 1 {
		t.Errorf("Pop() = %v, want 1", val1)
	}
	
	val2 := q.Pop()
	if val2 != "hello" {
		t.Errorf("Pop() = %v, want 'hello'", val2)
	}
	
	val3 := q.Pop()
	if val3 != 3.14 {
		t.Errorf("Pop() = %v, want 3.14", val3)
	}
	
	val4 := q.Pop()
	if val4 != true {
		t.Errorf("Pop() = %v, want true", val4)
	}
	
	val5 := q.Pop()
	slice, ok := val5.([]int)
	if !ok {
		t.Errorf("Pop() = %v, want []int", val5)
	}
	if len(slice) != 3 {
		t.Errorf("Pop() slice length = %d, want 3", len(slice))
	}
}

func TestQueue_Large(t *testing.T) {
	q := NewQueue()
	
	// Push large number of items
	count := 10000
	for i := 0; i < count; i++ {
		q.Push(i)
	}
	
	// Pop all and verify
	for i := 0; i < count; i++ {
		val := q.Pop()
		if val != i {
			t.Errorf("Pop() = %v, want %d", val, i)
		}
	}
	
	if !q.Empty() {
		t.Error("Queue should be empty after popping all items")
	}
}

func TestQueue_NilValues(t *testing.T) {
	q := NewQueue()
	
	// Push nil
	q.Push(nil)
	if q.Empty() {
		t.Error("Queue should not be empty after Push(nil)")
	}
	
	val := q.Pop()
	if val != nil {
		t.Errorf("Pop() = %v, want nil", val)
	}
	
	// Push multiple nils
	for i := 0; i < 5; i++ {
		q.Push(nil)
	}
	
	for i := 0; i < 5; i++ {
		val := q.Pop()
		if val != nil {
			t.Errorf("Pop() = %v, want nil", val)
		}
	}
}

func TestQueue_EmptyAfterPop(t *testing.T) {
	q := NewQueue()
	
	q.Push(1)
	q.Pop()
	
	if !q.Empty() {
		t.Error("Queue should be empty after Push and Pop")
	}
	
	// Pop from empty queue should return nil
	val := q.Pop()
	if val != nil {
		t.Errorf("Pop() from empty queue = %v, want nil", val)
	}
}

func TestQueue_Stress(t *testing.T) {
	q := NewQueue()
	
	// Stress test: push and pop in rapid succession
	for round := 0; round < 100; round++ {
		// Push batch
		for i := 0; i < 100; i++ {
			q.Push(round*100 + i)
		}
		
		// Pop batch
		for i := 0; i < 100; i++ {
			val := q.Pop()
			expected := round*100 + i
			if val != expected {
				t.Errorf("Round %d, Pop() = %v, want %d", round, val, expected)
			}
		}
		
		if !q.Empty() {
			t.Errorf("Queue should be empty after round %d", round)
		}
	}
}
