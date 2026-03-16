package gatetrans

import (
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

