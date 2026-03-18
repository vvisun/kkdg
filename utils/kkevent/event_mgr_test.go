package kkevent

import (
	"sync"
	"sync/atomic"
	"testing"
)

func evtListenerNoop(_ any) {}

var testCalled1 atomic.Int32
var testCalled2 atomic.Int32

func evtListener1(_ any) { testCalled1.Add(1) }
func evtListener2(_ any) { testCalled2.Add(1) }

func TestEventManager_AddListener_DedupAndID(t *testing.T) {
	mgr := NewEventManager[string]()
	testCalled1.Store(0)

	id1 := mgr.AddListener("k", evtListener1)
	if id1 == 0 {
		t.Fatalf("expected non-zero listener id")
	}

	id2 := mgr.AddListener("k", evtListener1) // dedup, should return same id
	if id2 != id1 {
		t.Fatalf("expected same id on dedup, got %d and %d", id1, id2)
	}

	mgr.Publish("k", 123)
	if testCalled1.Load() != 1 {
		t.Fatalf("expected listener called once, got %d", testCalled1.Load())
	}
}

func TestEventManager_RemoveListener(t *testing.T) {
	mgr := NewEventManager[int]()
	testCalled1.Store(0)
	testCalled2.Store(0)

	mgr.AddListener(1, evtListener1)
	mgr.AddListener(1, evtListener2)
	mgr.RemoveListener(1, evtListener1)

	mgr.Publish(1, "x")
	if testCalled1.Load() != 0 {
		t.Fatalf("expected removed listener not called, got %d", testCalled1.Load())
	}
	if testCalled2.Load() != 1 {
		t.Fatalf("expected remaining listener called once, got %d", testCalled2.Load())
	}
}

func TestEventManager_RemoveListenerByID(t *testing.T) {
	mgr := NewEventManager[int]()
	testCalled1.Store(0)

	id1 := mgr.AddListener(1, evtListenerNoop)
	_ = id1
	id2 := mgr.AddListener(1, evtListener1)

	mgr.RemoveListenerByID(1, id2)
	mgr.Publish(1, "x")

	if testCalled1.Load() != 0 {
		t.Fatalf("expected listener removed by id not called, got %d", testCalled1.Load())
	}
}

func TestEventManager_RemoveAllListeners(t *testing.T) {
	mgr := NewEventManager[string]()
	testCalled1.Store(0)

	mgr.AddListener("k", evtListener1)
	mgr.RemoveAllListeners("k")
	mgr.Publish("k", 1)

	if testCalled1.Load() != 0 {
		t.Fatalf("expected no listener called after RemoveAllListeners, got %d", testCalled1.Load())
	}
}

func TestEventManager_Publish_PanicDoesNotBreakOthers(t *testing.T) {
	mgr := NewEventManager[string]()
	var called atomic.Int32

	mgr.AddListener("k", func(data any) { panic("boom") })
	mgr.AddListener("k", func(data any) { called.Add(1) })

	// should not panic, and second should still run
	mgr.Publish("k", 1)
	if called.Load() != 1 {
		t.Fatalf("expected subsequent listener to run even if previous panics, got %d", called.Load())
	}
}

func TestEventManager_Publish_ConcurrentAddRemove(t *testing.T) {
	mgr := NewEventManager[int]()
	stop := atomic.Bool{}

	l := func(data any) {}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for !stop.Load() {
			mgr.AddListener(1, l)
			mgr.RemoveListener(1, l)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 10000; i++ {
			mgr.Publish(1, i)
		}
		stop.Store(true)
	}()

	wg.Wait()
}

func BenchmarkEventManager_Publish_NoListener(b *testing.B) {
	mgr := NewEventManager[int]()
	for i := 0; i < b.N; i++ {
		mgr.Publish(1, i)
	}
}

func BenchmarkEventManager_Publish_OneListener(b *testing.B) {
	mgr := NewEventManager[int]()
	mgr.AddListener(1, evtListenerNoop)
	for i := 0; i < b.N; i++ {
		mgr.Publish(1, i)
	}
}

func BenchmarkEventManager_Publish_FourListeners(b *testing.B) {
	mgr := NewEventManager[int]()
	mgr.AddListener(1, evtListenerNoop)
	mgr.AddListener(1, func(any) {})
	mgr.AddListener(1, func(any) {})
	mgr.AddListener(1, func(any) {})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mgr.Publish(1, i)
	}
}

func BenchmarkEventManager_AddRemoveListener(b *testing.B) {
	mgr := NewEventManager[int]()
	l := evtListenerNoop
	for i := 0; i < b.N; i++ {
		mgr.AddListener(1, l)
		mgr.RemoveListener(1, l)
	}
}


