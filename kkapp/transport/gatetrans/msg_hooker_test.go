package gatetrans

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/vvisun/kkdg/kknet/kkpacket"
)

// simpleMsgID is an arbitrary message id for tests.
const simpleMsgID kkpacket.MSGID = 1001

func TestMsgHooker_AddAndNotify(t *testing.T) {
	h := NewMsgHooker()

	var called int32
	listener := func(msgId kkpacket.MSGID, data any) {
		if msgId != simpleMsgID {
			t.Errorf("unexpected msgId, got=%d want=%d", msgId, simpleMsgID)
		}
		if s, ok := data.(string); !ok || s != "hello" {
			t.Errorf("unexpected data, got=%v", data)
		}
		atomic.AddInt32(&called, 1)
	}

	h.AddListener(listener)
	h.Notify(simpleMsgID, "hello")

	if atomic.LoadInt32(&called) != 1 {
		t.Fatalf("listener should be called once, got=%d", called)
	}
}

func TestMsgHooker_AddDuplicateListener(t *testing.T) {
	h := NewMsgHooker()

	var called int32
	listener := func(msgId kkpacket.MSGID, data any) {
		atomic.AddInt32(&called, 1)
	}

	h.AddListener(listener)
	h.AddListener(listener) // duplicate, should be ignored

	h.Notify(simpleMsgID, nil)

	if atomic.LoadInt32(&called) != 1 {
		t.Fatalf("duplicate listener should not be added, got calls=%d", called)
	}
}

func TestMsgHooker_RemoveListener(t *testing.T) {
	h := NewMsgHooker()

	var called1, called2 int32
	l1 := func(msgId kkpacket.MSGID, data any) {
		atomic.AddInt32(&called1, 1)
	}
	l2 := func(msgId kkpacket.MSGID, data any) {
		atomic.AddInt32(&called2, 1)
	}

	h.AddListener(l1)
	h.AddListener(l2)

	h.RemoveListener(l1)
	h.Notify(simpleMsgID, nil)

	if atomic.LoadInt32(&called1) != 0 {
		t.Fatalf("removed listener should not be called, got=%d", called1)
	}
	if atomic.LoadInt32(&called2) != 1 {
		t.Fatalf("remaining listener should be called once, got=%d", called2)
	}
}

func TestMsgHooker_RemoveAllListeners(t *testing.T) {
	h := NewMsgHooker()

	var called int32
	l := func(msgId kkpacket.MSGID, data any) {
		atomic.AddInt32(&called, 1)
	}

	h.AddListener(l)
	h.RemoveAllListeners()
	h.Notify(simpleMsgID, nil)

	if atomic.LoadInt32(&called) != 0 {
		t.Fatalf("no listener should be called after RemoveAllListeners, got=%d", called)
	}
}

func TestMsgHooker_NotifyPanicSafe(t *testing.T) {
	h := NewMsgHooker()

	var safeCalled int32

	// First listener panics.
	h.AddListener(func(msgId kkpacket.MSGID, data any) {
		panic("boom")
	})

	// Second listener should still be called.
	h.AddListener(func(msgId kkpacket.MSGID, data any) {
		atomic.AddInt32(&safeCalled, 1)
	})

	h.Notify(simpleMsgID, nil)

	if atomic.LoadInt32(&safeCalled) != 1 {
		t.Fatalf("panic in one listener should not stop others, safeCalled=%d", safeCalled)
	}
}

// ----------------- Benchmarks -----------------

// Benchmark single-listener Notify 性能。
func BenchmarkMsgHooker_Notify_OneListener(b *testing.B) {
	h := NewMsgHooker()

	var count int32
	h.AddListener(func(msgId kkpacket.MSGID, data any) {
		atomic.AddInt32(&count, 1)
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Notify(simpleMsgID, "payload")
	}
}

// Benchmark 多 listener 下的 Notify 性能。
func BenchmarkMsgHooker_Notify_ManyListeners(b *testing.B) {
	const listenerCount = 64
	h := NewMsgHooker()

	var count int32
	for i := 0; i < listenerCount; i++ {
		h.AddListener(func(msgId kkpacket.MSGID, data any) {
			atomic.AddInt32(&count, 1)
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Notify(simpleMsgID, "payload")
	}
}

// Benchmark 在高并发场景下频繁 Notify 的性能。
func BenchmarkMsgHooker_Notify_Parallel(b *testing.B) {
	h := NewMsgHooker()

	var count int64
	h.AddListener(func(msgId kkpacket.MSGID, data any) {
		atomic.AddInt64(&count, 1)
	})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			h.Notify(simpleMsgID, "payload")
		}
	})
}

// Benchmark AddListener / RemoveListener 的基本性能。
func BenchmarkMsgHooker_AddRemove(b *testing.B) {
	h := NewMsgHooker()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l := func(msgId kkpacket.MSGID, data any) {}
		h.AddListener(l)
		h.RemoveListener(l)
	}
}

// BenchmarkMsgHooker_AddListener_Parallel 模拟并发注册监听的场景。
func BenchmarkMsgHooker_AddListener_Parallel(b *testing.B) {
	h := NewMsgHooker()

	var wg sync.WaitGroup
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l := func(msgId kkpacket.MSGID, data any) {}
			h.AddListener(l)
		}(i)
	}
	wg.Wait()
}

