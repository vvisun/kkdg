package kkevent

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestEventBus_SubscribePublish(t *testing.T) {
	bus := NewEventBus().(*EventBus)
	var got1 atomic.Int32
	var got2 atomic.Int32

	_, err := bus.Subscribe("t", func(a int, b string) {
		if a == 7 && b == "x" {
			got1.Add(1)
		}
	})
	if err != nil {
		t.Fatalf("Subscribe error: %v", err)
	}
	// note: EventBus assumes all handlers under the same topic share the same signature
	_, err = bus.Subscribe("t", func(a int, b string) {
		if a == 7 && b == "x" {
			got2.Add(1)
		}
	})
	if err != nil {
		t.Fatalf("Subscribe error: %v", err)
	}

	bus.Publish("t", 7, "x")
	if got1.Load() != 1 {
		t.Fatalf("expected typed listener called once, got %d", got1.Load())
	}
	if got2.Load() != 1 {
		t.Fatalf("expected second typed listener called once, got %d", got2.Load())
	}
}

func TestEventBus_Subscribe_DedupSameFuncReturnsSameID(t *testing.T) {
	bus := NewEventBus().(*EventBus)
	called := atomic.Int32{}

	fn := func() { called.Add(1) }
	id1, err := bus.Subscribe("t", fn)
	if err != nil {
		t.Fatalf("Subscribe error: %v", err)
	}
	id2, err := bus.Subscribe("t", fn)
	if err != nil {
		t.Fatalf("Subscribe error: %v", err)
	}
	if id1 == 0 || id2 == 0 || id1 != id2 {
		t.Fatalf("expected same non-zero id on dedup, got id1=%d id2=%d", id1, id2)
	}

	bus.Publish("t")
	if called.Load() != 1 {
		t.Fatalf("expected listener called once, got %d", called.Load())
	}
}

func TestEventBus_UnsubscribeByHandler(t *testing.T) {
	bus := NewEventBus().(*EventBus)
	called1 := atomic.Int32{}
	called2 := atomic.Int32{}

	fn1 := func() { called1.Add(1) }
	fn2 := func() { called2.Add(1) }

	_, _ = bus.Subscribe("t", fn1)
	_, _ = bus.Subscribe("t", fn2)

	if err := bus.Unsubscribe("t", fn1); err != nil {
		t.Fatalf("Unsubscribe error: %v", err)
	}

	bus.Publish("t")
	if called1.Load() != 0 {
		t.Fatalf("expected unsubscribed fn1 not called, got %d", called1.Load())
	}
	if called2.Load() != 1 {
		t.Fatalf("expected fn2 called once, got %d", called2.Load())
	}
}

func TestEventBus_UnsubscribeByID(t *testing.T) {
	bus := NewEventBus().(*EventBus)
	called := atomic.Int32{}

	id1, _ := bus.Subscribe("t", func() {})
	id2, _ := bus.Subscribe("t", func() { called.Add(1) })

	_ = id1
	if err := bus.UnsubscribeByID("t", id2); err != nil {
		t.Fatalf("UnsubscribeByID error: %v", err)
	}

	bus.Publish("t")
	if called.Load() != 0 {
		t.Fatalf("expected removed listener not called, got %d", called.Load())
	}
}

func TestEventBus_UnsubscribeAllAndHasCallback(t *testing.T) {
	bus := NewEventBus().(*EventBus)
	_, _ = bus.Subscribe("t", func() {})
	if !bus.HasCallback("t") {
		t.Fatalf("expected HasCallback=true after Subscribe")
	}
	if err := bus.UnsubscribeAll("t"); err != nil {
		t.Fatalf("UnsubscribeAll error: %v", err)
	}
	if bus.HasCallback("t") {
		t.Fatalf("expected HasCallback=false after UnsubscribeAll")
	}
}

func TestEventBus_SubscribeOnce_RemovedAfterFirstPublish(t *testing.T) {
	bus := NewEventBus().(*EventBus)
	called := atomic.Int32{}

	_, err := bus.SubscribeOnce("t", func() { called.Add(1) })
	if err != nil {
		t.Fatalf("SubscribeOnce error: %v", err)
	}

	bus.Publish("t")
	bus.Publish("t")

	if called.Load() != 1 {
		t.Fatalf("expected once listener called once, got %d", called.Load())
	}
	if bus.HasCallback("t") {
		t.Fatalf("expected once listener removed after publish")
	}
}

func TestEventBus_SubscribeAsync_WaitAsync(t *testing.T) {
	bus := NewEventBus().(*EventBus)
	started := make(chan struct{})
	done := make(chan struct{})

	_, err := bus.SubscribeAsync("t", func() {
		close(started)
		time.Sleep(30 * time.Millisecond)
		close(done)
	}, false)
	if err != nil {
		t.Fatalf("SubscribeAsync error: %v", err)
	}

	bus.Publish("t")
	<-started
	bus.WaitAsync()

	select {
	case <-done:
		// ok
	default:
		t.Fatalf("expected async handler completed after WaitAsync")
	}
}

func TestEventBus_SubscribeAsync_TransactionalSerializes(t *testing.T) {
	bus := NewEventBus().(*EventBus)

	var inFlight atomic.Int32
	var overlap atomic.Int32

	fn := func() {
		if inFlight.Add(1) > 1 {
			overlap.Store(1)
		}
		time.Sleep(5 * time.Millisecond)
		inFlight.Add(-1)
	}

	_, err := bus.SubscribeAsync("t", fn, true)
	if err != nil {
		t.Fatalf("SubscribeAsync error: %v", err)
	}

	// fire multiple publishes; transactional async handler should not overlap
	for i := 0; i < 20; i++ {
		bus.Publish("t")
	}
	bus.WaitAsync()

	if overlap.Load() != 0 {
		t.Fatalf("expected transactional async handler not overlapping")
	}
}

func TestEventBus_Publish_TypeMismatchUsesZeroValue(t *testing.T) {
	bus := NewEventBus().(*EventBus)
	var gotA atomic.Int32
	var gotB atomic.Int32

	_, err := bus.Subscribe("t", func(a int, b string) {
		if a == 0 {
			gotA.Add(1)
		}
		if b == "" {
			gotB.Add(1)
		}
	})
	if err != nil {
		t.Fatalf("Subscribe error: %v", err)
	}

	// wrong types should be replaced with zero values by setUpPublish
	bus.Publish("t", "not-int", 123)
	if gotA.Load() != 1 || gotB.Load() != 1 {
		t.Fatalf("expected zero values when type mismatch, gotA=%d gotB=%d", gotA.Load(), gotB.Load())
	}
}

func TestEventBus_Publish_MixedSignatures_IsRecovered(t *testing.T) {
	bus := NewEventBus().(*EventBus)
	var got atomic.Int32

	// first handler expects 2 args; second expects 0 args -> reflect will panic but Publish should recover
	_, _ = bus.Subscribe("t", func(a int, b string) { got.Add(1) })
	_, _ = bus.Subscribe("t", func() { got.Add(1000) })

	bus.Publish("t", 1, "x")

	// we only assert no panic; behavior for mixed signatures isn't guaranteed
	if got.Load() == 0 {
		t.Fatalf("expected at least first handler executed before panic")
	}
}

func TestEventBus_Publish_PanicIsRecovered(t *testing.T) {
	bus := NewEventBus().(*EventBus)

	_, err := bus.Subscribe("t", func() { panic("boom") })
	if err != nil {
		t.Fatalf("Subscribe error: %v", err)
	}

	// should not panic
	bus.Publish("t")
}

func TestEventBus_Publish_ConcurrentAddRemove_NoPanic(t *testing.T) {
	bus := NewEventBus().(*EventBus)
	stop := atomic.Bool{}
	var wg sync.WaitGroup

	fn := func() {}

	wg.Add(2)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			_, _ = bus.Subscribe("t", fn)
			_ = bus.Unsubscribe("t", fn)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 5000; i++ {
			bus.Publish("t")
		}
		stop.Store(true)
	}()

	wg.Wait()
}

func BenchmarkEventBus_Publish_NoHandler(b *testing.B) {
	bus := NewEventBus().(*EventBus)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.Publish("t", 1, "x")
	}
}

func BenchmarkEventBus_Publish_OneSyncHandler(b *testing.B) {
	bus := NewEventBus().(*EventBus)
	_, _ = bus.Subscribe("t", func(a int, s string) {})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.Publish("t", 1, "x")
	}
}

func BenchmarkEventBus_Publish_FourSyncHandlers(b *testing.B) {
	bus := NewEventBus().(*EventBus)
	_, _ = bus.Subscribe("t", func(a int, s string) {})
	_, _ = bus.Subscribe("t", func(a int, s string) {})
	_, _ = bus.Subscribe("t", func(a int, s string) {})
	_, _ = bus.Subscribe("t", func(a int, s string) {})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.Publish("t", 1, "x")
	}
}

func BenchmarkEventBus_SubscribeUnsubscribe(b *testing.B) {
	bus := NewEventBus().(*EventBus)
	fn := func(a int, s string) {}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = bus.Subscribe("t", fn)
		_ = bus.Unsubscribe("t", fn)
	}
}

func BenchmarkEventBus_Publish_OneAsyncHandler(b *testing.B) {
	bus := NewEventBus().(*EventBus)
	_, _ = bus.SubscribeAsync("t", func(a int, s string) {}, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.Publish("t", 1, "x")
	}
	b.StopTimer()
	bus.WaitAsync()
}

