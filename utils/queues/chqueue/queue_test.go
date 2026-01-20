package chqueue

import (
	"sync"
	"testing"
	"time"
)

func TestQueue_New(t *testing.T) {
	q := NewQueue()
	if q.head == nil {
		t.Error("NewQueue() head should not be nil")
	}
	if q.tail == nil {
		t.Error("NewQueue() tail should not be nil")
	}
	if q.C == nil {
		t.Error("NewQueue() channel C should not be nil")
	}
	if q.Count() != 0 {
		t.Errorf("NewQueue() count = %d, want 0", q.Count())
	}
	if !q.Empty() {
		t.Error("NewQueue() should be empty")
	}
}

func TestQueue_PushPop(t *testing.T) {
	q := NewQueue()

	// Push single item
	q.Push(1)
	if q.Count() != 1 {
		t.Errorf("Count() = %d, want 1", q.Count())
	}
	if q.Empty() {
		t.Error("Queue should not be empty after Push")
	}

	// Pop item
	val := q.Pop()
	if val != 1 {
		t.Errorf("Pop() = %v, want 1", val)
	}
	if q.Count() != 0 {
		t.Errorf("Count() = %d, want 0", q.Count())
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

	if q.Count() != 10 {
		t.Errorf("Count() = %d, want 10", q.Count())
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
	if q.Count() != 0 {
		t.Errorf("Count() = %d, want 0", q.Count())
	}
}

func TestQueue_PopEmpty(t *testing.T) {
	q := NewQueue()

	val := q.Pop()
	if val != nil {
		t.Errorf("Pop() on empty queue = %v, want nil", val)
	}
	if q.Count() != 0 {
		t.Errorf("Count() = %d, want 0", q.Count())
	}
}

func TestQueue_PushNil(t *testing.T) {
	q := NewQueue()

	// Push nil should be ignored
	q.Push(nil)
	if q.Count() != 0 {
		t.Errorf("Push(nil) should not increment count, got %d", q.Count())
	}
	if !q.Empty() {
		t.Error("Queue should be empty after Push(nil)")
	}

	// Push multiple nils
	for i := 0; i < 5; i++ {
		q.Push(nil)
	}
	if q.Count() != 0 {
		t.Errorf("Multiple Push(nil) should not increment count, got %d", q.Count())
	}

	// Push valid value after nil
	q.Push(42)
	if q.Count() != 1 {
		t.Errorf("Count() = %d, want 1", q.Count())
	}
	val := q.Pop()
	if val != 42 {
		t.Errorf("Pop() = %v, want 42", val)
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

func TestQueue_Count(t *testing.T) {
	q := NewQueue()

	// Verify initial count
	if q.Count() != 0 {
		t.Errorf("Initial Count() = %d, want 0", q.Count())
	}

	// Push and verify count
	for i := 1; i <= 10; i++ {
		q.Push(i)
		if q.Count() != int32(i) {
			t.Errorf("After Push(%d), Count() = %d, want %d", i, q.Count(), i)
		}
	}

	// Pop and verify count
	for i := 9; i >= 0; i-- {
		q.Pop()
		if q.Count() != int32(i) {
			t.Errorf("After Pop(), Count() = %d, want %d", q.Count(), i)
		}
	}
}

func TestQueue_ConcurrentPush(t *testing.T) {
	q := NewQueue()
	const numGoroutines = 10
	const itemsPerGoroutine = 100
	var wg sync.WaitGroup

	// Concurrent push
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < itemsPerGoroutine; j++ {
				q.Push(id*itemsPerGoroutine + j)
			}
		}(i)
	}

	wg.Wait()

	// Verify total count
	expectedCount := int32(numGoroutines * itemsPerGoroutine)
	if q.Count() != expectedCount {
		t.Errorf("Count() = %d, want %d", q.Count(), expectedCount)
	}

	// Pop all items and verify we got all of them
	popped := make(map[int]bool)
	for i := 0; i < numGoroutines*itemsPerGoroutine; i++ {
		val := q.Pop()
		if val == nil {
			t.Errorf("Pop() returned nil at iteration %d", i)
			continue
		}
		popped[val.(int)] = true
	}

	// Verify we got all unique values
	if len(popped) != numGoroutines*itemsPerGoroutine {
		t.Errorf("Got %d unique values, want %d", len(popped), numGoroutines*itemsPerGoroutine)
	}
}

func TestQueue_ConcurrentPushPop(t *testing.T) {
	q := NewQueue()
	const numItems = 1000
	var wg sync.WaitGroup

	// Concurrent push
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numItems; i++ {
			q.Push(i)
		}
	}()

	// Concurrent pop
	popped := make(chan int, numItems)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numItems; i++ {
			val := q.Pop()
			if val != nil {
				popped <- val.(int)
			} else {
				// Wait a bit and retry
				time.Sleep(time.Microsecond)
				i--
			}
		}
	}()

	wg.Wait()
	close(popped)

	// Verify we got all items
	received := make(map[int]bool)
	for val := range popped {
		received[val] = true
	}

	if len(received) != numItems {
		t.Errorf("Received %d items, want %d", len(received), numItems)
	}
}

func TestQueue_ChannelC(t *testing.T) {
	q := NewQueue()

	// Channel should be buffered and non-blocking for sends
	select {
	case <-q.C:
		// Should not receive anything initially
		t.Error("Channel should not have value initially")
	default:
		// Good, channel is empty
	}

	// Push item and check channel
	q.Push(1)
	select {
	case count := <-q.C:
		if count <= 0 {
			t.Errorf("Channel should receive positive count, got %d", count)
		}
	default:
		// Channel might be full, which is ok
	}

	// Pop item
	q.Pop()

	// Push multiple items
	for i := 0; i < 5; i++ {
		q.Push(i)
	}

	// Should be able to receive count from channel
	select {
	case count := <-q.C:
		if count <= 0 {
			t.Errorf("Channel should receive positive count, got %d", count)
		}
	default:
		// Channel might be full, which is ok
	}
}

func TestQueue_Destroy(t *testing.T) {
	q := NewQueue()

	// Push some items
	for i := 0; i < 5; i++ {
		q.Push(i)
	}

	// Destroy queue
	q.Destroy()

	// Channel should be closed - drain any buffered values and verify it's closed
	// Read until we get ok=false (channel closed)
	for {
		_, ok := <-q.C
		if !ok {
			break
		}
	}

	// Verify count is reset
	if q.Count() != 0 {
		t.Errorf("Count() = %d, want 0 after Destroy()", q.Count())
	}

	// Verify head and tail are nil
	if q.head != nil {
		t.Error("head should be nil after Destroy()")
	}
	if q.tail != nil {
		t.Error("tail should be nil after Destroy()")
	}
}

func TestQueue_DifferentTypes(t *testing.T) {
	q := NewQueue()

	// Test comparable value types
	testCases := []struct {
		name string
		val  interface{}
	}{
		{"int", 42},
		{"string", "hello"},
		{"bool", true},
		{"struct", struct{ x int }{x: 10}},
		{"pointer", new(int)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			q.Push(tc.val)
			popped := q.Pop()
			if popped != tc.val {
				t.Errorf("Pop() = %v, want %v", popped, tc.val)
			}
		})
	}

	// Test non-comparable types separately
	q2 := NewQueue()
	sliceVal := []int{1, 2, 3}
	q2.Push(sliceVal)
	poppedSlice := q2.Pop()
	if poppedSlice == nil {
		t.Error("Pop() returned nil for slice")
	}
	// Can't compare slices directly, but we can verify it's not nil

	q3 := NewQueue()
	mapVal := map[string]int{"a": 1}
	q3.Push(mapVal)
	poppedMap := q3.Pop()
	if poppedMap == nil {
		t.Error("Pop() returned nil for map")
	}
	// Can't compare maps directly, but we can verify it's not nil
}

func TestQueue_Order(t *testing.T) {
	q := NewQueue()

	// Push items in order
	for i := 0; i < 100; i++ {
		q.Push(i)
	}

	// Pop items and verify FIFO order
	for i := 0; i < 100; i++ {
		val := q.Pop()
		if val != i {
			t.Errorf("Pop() = %v, want %d (FIFO order)", val, i)
		}
	}
}

func TestQueue_Stress(t *testing.T) {
	q := NewQueue()
	const numOperations = 10000

	// Stress test with many operations
	for i := 0; i < numOperations; i++ {
		q.Push(i)
		if i%2 == 0 {
			val := q.Pop()
			if val == nil {
				t.Errorf("Pop() returned nil at iteration %d", i)
			}
		}
	}

	// Pop remaining items
	remaining := q.Count()
	for i := int32(0); i < remaining; i++ {
		val := q.Pop()
		if val == nil {
			t.Errorf("Pop() returned nil when popping remaining items")
		}
	}

	if !q.Empty() {
		t.Error("Queue should be empty after stress test")
	}
}
