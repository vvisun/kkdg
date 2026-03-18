package kkevent

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestSpecEventManager_Subscribe_DedupAndPublish(t *testing.T) {
	mgr := NewSpecEventManager[string, int]()
	var called atomic.Int32

	fn := func(v int) {
		if v == 7 {
			called.Add(1)
		}
	}

	id1 := mgr.Subscribe("k", fn)
	if id1 == 0 {
		t.Fatalf("expected non-zero id")
	}
	id2 := mgr.Subscribe("k", fn)
	if id2 != id1 {
		t.Fatalf("expected dedup id to be same, id1=%d id2=%d", id1, id2)
	}

	mgr.Publish("k", 7)
	if called.Load() != 1 {
		t.Fatalf("expected called once, got %d", called.Load())
	}
}

func TestSpecEventManager_Unsubscribe(t *testing.T) {
	mgr := NewSpecEventManager[string, int]()
	var called1 atomic.Int32
	var called2 atomic.Int32

	fn1 := func(int) { called1.Add(1) }
	fn2 := func(int) { called2.Add(1) }

	_ = mgr.Subscribe("k", fn1)
	_ = mgr.Subscribe("k", fn2)
	mgr.Unsubscribe("k", fn1)

	mgr.Publish("k", 1)
	if called1.Load() != 0 {
		t.Fatalf("expected unsubscribed fn1 not called, got %d", called1.Load())
	}
	if called2.Load() != 1 {
		t.Fatalf("expected fn2 called once, got %d", called2.Load())
	}
}

func TestSpecEventManager_UnsubscribeByID(t *testing.T) {
	mgr := NewSpecEventManager[int, string]()
	var called atomic.Int32

	_ = mgr.Subscribe(1, func(string) {})
	id2 := mgr.Subscribe(1, func(s string) { called.Add(1) })
	if id2 == 0 {
		t.Fatalf("expected non-zero id2")
	}

	mgr.UnsubscribeByID(1, id2)
	mgr.Publish(1, "x")

	if called.Load() != 0 {
		t.Fatalf("expected listener removed by id not called, got %d", called.Load())
	}
}

func TestSpecEventManager_UnsubscribeAll(t *testing.T) {
	mgr := NewSpecEventManager[string, int]()
	var called atomic.Int32

	_ = mgr.Subscribe("k", func(int) { called.Add(1) })
	mgr.UnsubscribeAll("k")
	mgr.Publish("k", 1)

	if called.Load() != 0 {
		t.Fatalf("expected no calls after UnsubscribeAll, got %d", called.Load())
	}
}

func TestSpecEventManager_Publish_PanicRecovered(t *testing.T) {
	mgr := NewSpecEventManager[string, int]()
	var called atomic.Int32

	_ = mgr.Subscribe("k", func(int) { panic("boom") })
	_ = mgr.Subscribe("k", func(int) { called.Add(1) })

	// should not panic
	mgr.Publish("k", 1)
	if called.Load() != 1 {
		t.Fatalf("expected second listener called even if first panics, got %d", called.Load())
	}
}

func TestSpecEventManager_ConcurrentSubscribeUnsubscribeAndPublish(t *testing.T) {
	mgr := NewSpecEventManager[int, int]()
	stop := atomic.Bool{}

	fn := func(int) {}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			_ = mgr.Subscribe(1, fn)
			mgr.Unsubscribe(1, fn)
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

func BenchmarkSpecEventManager_Publish_NoListener(b *testing.B) {
	mgr := NewSpecEventManager[int, int]()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mgr.Publish(1, i)
	}
}

func BenchmarkSpecEventManager_Publish_OneListener(b *testing.B) {
	mgr := NewSpecEventManager[int, int]()
	mgr.Subscribe(1, func(int) {})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mgr.Publish(1, i)
	}
}

func BenchmarkSpecEventManager_Publish_FourListeners(b *testing.B) {
	mgr := NewSpecEventManager[int, int]()
	mgr.Subscribe(1, func(int) {})
	mgr.Subscribe(1, func(int) {})
	mgr.Subscribe(1, func(int) {})
	mgr.Subscribe(1, func(int) {})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mgr.Publish(1, i)
	}
}

func BenchmarkSpecEventManager_SubscribeUnsubscribe(b *testing.B) {
	mgr := NewSpecEventManager[int, int]()
	fn := func(int) {}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mgr.Subscribe(1, fn)
		mgr.Unsubscribe(1, fn)
	}
}


