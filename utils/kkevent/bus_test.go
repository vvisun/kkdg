package kkevent

import (
	"sync"
	"testing"
	"time"
)

func TestSubscribe(t *testing.T) {
	bus := NewEventBus()
	topic := "test-topic"
	called := false
	handler := func(data string) {
		called = true
		if data != "hello" {
			t.Errorf("expected hello, got %s", data)
		}
	}

	bus.Subscribe(topic, handler)
	bus.Publish(topic, "hello")

	if !called {
		t.Error("handler was not called")
	}
}

func TestSubscribeOnce(t *testing.T) {
	bus := NewEventBus()
	topic := "test-once"
	callCount := 0
	handler := func() {
		callCount++
	}

	bus.SubscribeOnce(topic, handler)
	bus.Publish(topic)
	bus.Publish(topic)

	if callCount != 1 {
		t.Errorf("expected 1 call, got %d", callCount)
	}
}

func TestUnsubscribe(t *testing.T) {
	bus := NewEventBus()
	topic := "test-unsubscribe"
	callCount := 0
	handler := func() {
		callCount++
	}

	bus.Subscribe(topic, handler)
	bus.Publish(topic)
	err := bus.Unsubscribe(topic, handler)
	if err != nil {
		t.Fatalf("unsubscribe failed: %v", err)
	}
	bus.Publish(topic)

	if callCount != 1 {
		t.Errorf("expected 1 call, got %d", callCount)
	}
}

func TestSubscribeAsync(t *testing.T) {
	bus := NewEventBus()
	topic := "test-async"
	wg := sync.WaitGroup{}
	wg.Add(1)
	called := false

	handler := func() {
		called = true
		wg.Done()
	}

	bus.SubscribeAsync(topic, handler, false)
	bus.Publish(topic)
	wg.Wait()

	if !called {
		t.Error("async handler was not called")
	}
}

func TestTransactionalAsync(t *testing.T) {
	bus := NewEventBus()
	topic := "test-transactional"

	results := make([]int, 0)
	mu := sync.Mutex{}

	handler := func(val int) {
		mu.Lock()
		results = append(results, val)
		mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}

	// Transactional async: handlers for the SAME topic are run serially
	// Actually, looking at the code:
	// if handler.transactional {
	//     bus.lock.Unlock()
	//     handler.Lock()
	//     bus.lock.Lock()
	// }
	// go bus.doPublishAsync(handler, topic, args...)
	// AND doPublishAsync does:
	// if handler.transactional {
	//     defer handler.Unlock()
	// }
	// So it locks the specific handler. If we publish twice, the second publish's execution of THIS handler will wait for the first.

	bus.SubscribeAsync(topic, handler, true)
	bus.Publish(topic, 1)
	bus.Publish(topic, 2)

	bus.WaitAsync()

	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}
	if results[0] != 1 || results[1] != 2 {
		t.Errorf("results order mismatch: %v", results)
	}
}

func TestMultipleSubscribers(t *testing.T) {
	bus := NewEventBus()
	topic := "test-multi"
	count1 := 0
	count2 := 0

	handler1 := func() { count1++ }
	handler2 := func() { count2++ }

	bus.Subscribe(topic, handler1)
	bus.Subscribe(topic, handler2)

	bus.Publish(topic)

	if count1 != 1 || count2 != 1 {
		t.Errorf("counts mismatch: %d, %d", count1, count2)
	}
}

func TestArgumentPassing(t *testing.T) {
	bus := NewEventBus()
	topic := "test-args"

	var rInt int
	var rStr string
	var rBool bool

	handler := func(i int, s string, b bool) {
		rInt = i
		rStr = s
		rBool = b
	}

	bus.Subscribe(topic, handler)
	bus.Publish(topic, 42, "world", true)

	if rInt != 42 || rStr != "world" || !rBool {
		t.Errorf("args mismatch: %d, %s, %v", rInt, rStr, rBool)
	}
}

func TestWaitAsync(t *testing.T) {
	bus := NewEventBus()
	topic := "test-wait"

	done := false
	handler := func() {
		time.Sleep(50 * time.Millisecond)
		done = true
	}

	bus.SubscribeAsync(topic, handler, false)
	bus.Publish(topic)

	bus.WaitAsync()

	if !done {
		t.Error("WaitAsync did not wait for completion")
	}
}

func TestHasCallback(t *testing.T) {
	bus := NewEventBus()
	topic := "test-has"

	if bus.HasCallback(topic) {
		t.Error("topic should not have callbacks yet")
	}

	handler := func() {}
	bus.Subscribe(topic, handler)

	if !bus.HasCallback(topic) {
		t.Error("topic should have callbacks")
	}

	bus.Unsubscribe(topic, handler)

	if bus.HasCallback(topic) {
		t.Error("topic should not have callbacks after unsubscribe")
	}
}

func TestPublishNil(t *testing.T) {
	bus := NewEventBus()
	topic := "test-nil"
	called := false
	handler := func(data *string) {
		called = true
		if data != nil {
			t.Error("expected nil")
		}
	}

	bus.Subscribe(topic, handler)
	bus.Publish(topic, nil)

	if !called {
		t.Error("handler was not called")
	}
}

func BenchmarkPublish(b *testing.B) {
	bus := NewEventBus()
	topic := "bench"
	handler := func(a int, b string, c bool) {}
	bus.Subscribe(topic, handler)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.Publish(topic, 1, "test", true)
	}
}

func BenchmarkPublishAsync(b *testing.B) {
	bus := NewEventBus()
	topic := "bench-async"
	handler := func() {}
	bus.SubscribeAsync(topic, handler, false)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.Publish(topic)
	}
	bus.WaitAsync()
}
