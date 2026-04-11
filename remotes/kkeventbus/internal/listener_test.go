package internal

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vvisun/kkdg/remotes/kkeventbus"
)

func TestListenerManager_Subscribe_nil(t *testing.T) {
	m := NewListenerManager()
	if id := m.Subscribe(nil); id != 0 {
		t.Fatalf("Subscribe(nil) = %d, want 0", id)
	}
	if m.GetListenerCount() != 0 {
		t.Fatalf("GetListenerCount = %d, want 0", m.GetListenerCount())
	}
}

func TestListenerManager_Subscribe_dedupeSameFunc(t *testing.T) {
	m := NewListenerManager()
	h := func(e *kkeventbus.Event) {}
	id1 := m.Subscribe(h)
	id2 := m.Subscribe(h)
	if id1 == 0 || id1 != id2 {
		t.Fatalf("ids %d %d, want same non-zero", id1, id2)
	}
	if m.GetListenerCount() != 1 {
		t.Fatalf("count %d, want 1", m.GetListenerCount())
	}
}

func TestListenerManager_Subscribe_twoDistinctFuncs(t *testing.T) {
	m := NewListenerManager()
	a := func(e *kkeventbus.Event) {}
	b := func(e *kkeventbus.Event) {}
	ida := m.Subscribe(a)
	idb := m.Subscribe(b)
	if ida == 0 || idb == 0 || ida == idb {
		t.Fatalf("ida=%d idb=%d, want distinct non-zero", ida, idb)
	}
	if m.GetListenerCount() != 2 {
		t.Fatalf("count %d, want 2", m.GetListenerCount())
	}
}

func TestListenerManager_UnsubscribeByID(t *testing.T) {
	m := NewListenerManager()
	h := func(e *kkeventbus.Event) {}
	id := m.Subscribe(h)
	m.UnsubscribeByID(id)
	if m.GetListenerCount() != 0 {
		t.Fatalf("count after UnsubscribeByID = %d", m.GetListenerCount())
	}
}

func TestListenerManager_Unsubscribe_func(t *testing.T) {
	m := NewListenerManager()
	h := func(e *kkeventbus.Event) {}
	m.Subscribe(h)
	m.Unsubscribe(h)
	if m.GetListenerCount() != 0 {
		t.Fatalf("count after Unsubscribe = %d", m.GetListenerCount())
	}
}

func TestListenerManager_Publish_invokesHandlers(t *testing.T) {
	m := NewListenerManager()
	var n atomic.Int32
	h1 := func(e *kkeventbus.Event) { n.Add(1) }
	h2 := func(e *kkeventbus.Event) { n.Add(1) }
	m.Subscribe(h1)
	m.Subscribe(h2)
	ev := &kkeventbus.Event{Topic: "t"}
	m.Publish(ev)
	waitUntil(t, func() bool { return n.Load() == 2 }, 2*time.Second)
	if n.Load() != 2 {
		t.Fatalf("handlers called %d times, want 2", n.Load())
	}
}

func TestListenerManager_UnsubscribeAll(t *testing.T) {
	m := NewListenerManager()
	m.Subscribe(func(e *kkeventbus.Event) {})
	m.Subscribe(func(e *kkeventbus.Event) {})
	m.UnsubscribeAll()
	if m.GetListenerCount() != 0 {
		t.Fatalf("count = %d", m.GetListenerCount())
	}
}

func waitUntil(t *testing.T, cond func() bool, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not met before deadline")
}

func TestListenerManager_Publish_snapshotNoDeadlock(t *testing.T) {
	m := NewListenerManager()
	var wg sync.WaitGroup
	wg.Add(1)
	h := func(e *kkeventbus.Event) {
		wg.Done()
	}
	m.Subscribe(h)
	m.Publish(&kkeventbus.Event{Topic: "x"})
	wg.Wait()
}
