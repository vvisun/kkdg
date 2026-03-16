package kkevent

import (
	"strings"
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

	if _, err := bus.SubscribeAsync(topic, handler, true); err != nil {
		t.Fatalf("SubscribeAsync: %v", err)
	}
	bus.Publish(topic, 1)
	bus.Publish(topic, 2)

	bus.WaitAsync()

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	seen := map[int]bool{}
	for _, v := range results {
		seen[v] = true
	}
	if !seen[1] || !seen[2] {
		t.Errorf("unexpected results content: %v", results)
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

// ----------------- Additional edge / error tests -----------------

// TestSubscribeInvalidHandler verifies that subscribing a non-func handler returns error.
func TestSubscribeInvalidHandler(t *testing.T) {
	bus := NewEventBus()
	_, err := bus.Subscribe("test-invalid", "not-a-func")
	if err == nil {
		t.Fatalf("expected error when subscribing non-func handler")
	}
	if !strings.Contains(err.Error(), "is not of type reflect.Func") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestUnsubscribeErrors covers Unsubscribe error branches: non-existent topic and handler-not-found.
func TestUnsubscribeErrors(t *testing.T) {
	bus := NewEventBus()
	topic := "test-unsub-errors"
	h1 := func() {}
	h2 := func() {}

	// topic not exist
	if err := bus.Unsubscribe("no-such-topic", h1); err == nil {
		t.Fatalf("expected error for non-existent topic")
	}

	// handler not found
	if _, err := bus.Subscribe(topic, h1); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := bus.Unsubscribe(topic, h2); err == nil {
		t.Fatalf("expected error for handler not found")
	}
}

// TestSubscribeOnceAsync verifies that SubscribeOnceAsync handlers fire only once even with async.
func TestSubscribeOnceAsync(t *testing.T) {
	bus := NewEventBus()
	topic := "test-once-async"
	var mu sync.Mutex
	count := 0

	handler := func() {
		mu.Lock()
		count++
		mu.Unlock()
	}

	if _, err := bus.SubscribeOnceAsync(topic, handler); err != nil {
		t.Fatalf("SubscribeOnceAsync: %v", err)
	}
	bus.Publish(topic)
	bus.Publish(topic)
	bus.WaitAsync()

	mu.Lock()
	defer mu.Unlock()
	if count != 1 {
		t.Fatalf("expected handler called once, got %d", count)
	}
}

// TestGlobalBus verifies the convenience wrappers around defaultBus.
func TestGlobalBus(t *testing.T) {
	topic := "test-global-bus"
	ch := make(chan struct{}, 1)

	if _, err := GlobalBus.Subscribe(topic, func(v int) {
		if v == 42 {
			ch <- struct{}{}
		}
	}); err != nil {
		t.Fatalf("Subscribe on global bus: %v", err)
	}

	GlobalBus.Publish(topic, 42)
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("global Publish/Subscribe did not deliver event in time")
	}
}

// TestConcurrentPublishSubscribe does a small race-style sanity check for
// concurrent subscribe/unsubscribe while publishing, exercising the COW logic.
func TestConcurrentPublishSubscribe(t *testing.T) {
	bus := NewEventBus()
	topic := "test-concurrent"

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// publisher
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				bus.Publish(topic, 1)
			}
		}
	}()

	// concurrent subscribe/unsubscribe
	wg.Add(1)
	go func() {
		defer wg.Done()
		handler := func(int) {}
		for i := 0; i < 1000; i++ {
			_, _ = bus.Subscribe(topic, handler)
			_ = bus.Unsubscribe(topic, handler)
		}
	}()

	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
}
