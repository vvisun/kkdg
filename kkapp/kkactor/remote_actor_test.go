package kkactor

import (
	"sync"
	"testing"

	"github.com/vvisun/kkdg/kkapp"
)

// fakeTransport 用于测试：实现 IRemoteActorTransport，记录调用便于断言。
type fakeTransport struct {
	mu         sync.Mutex
	inited     bool
	receiverFn func(from RemoteActorID, msg any)
	tellLog    []struct {
		target RemoteActorID
		msg    any
	}
	closed bool
}

func (f *fakeTransport) Init(self *kkapp.NodeInfo) error {
	f.mu.Lock()
	f.inited = true
	f.mu.Unlock()
	return nil
}

func (f *fakeTransport) SetReceiver(fn func(from RemoteActorID, msg any)) {
	f.mu.Lock()
	f.receiverFn = fn
	f.mu.Unlock()
}

func (f *fakeTransport) TellRemote(target RemoteActorID, msg any) error {
	f.mu.Lock()
	f.tellLog = append(f.tellLog, struct {
		target RemoteActorID
		msg    any
	}{target, msg})
	f.mu.Unlock()
	return nil
}

func (f *fakeTransport) Close() error {
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
	return nil
}

// 确保 fakeTransport 实现 IRemoteActorTransport（编译期检查）
var _ IRemoteActorTransport = (*fakeTransport)(nil)

func TestFakeTransport_IRemoteActorTransport(t *testing.T) {
	node := kkapp.NewNodeInfo("game1", "game", "127.0.0.1:8080", "", nil)
	ft := &fakeTransport{}

	if err := ft.Init(node); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if !ft.inited {
		t.Error("Init should set inited")
	}

	called := false
	ft.SetReceiver(func(from RemoteActorID, msg any) {
		called = true
	})
	if ft.receiverFn == nil {
		t.Error("SetReceiver should store callback")
	}
	ft.receiverFn(RemoteActorID{NodeID: "gate1", ActorKey: "x"}, "hello")
	if !called {
		t.Error("receiver callback should be invoked")
	}

	target := RemoteActorID{NodeID: "game2", ActorKey: "ccgame/main"}
	if err := ft.TellRemote(target, "ping"); err != nil {
		t.Fatalf("TellRemote: %v", err)
	}
	ft.mu.Lock()
	n := len(ft.tellLog)
	var entry struct {
		target RemoteActorID
		msg    any
	}
	if n >= 1 {
		entry = ft.tellLog[0]
	}
	ft.mu.Unlock()
	if n != 1 || entry.target != target || entry.msg != "ping" {
		t.Errorf("TellRemote log: n=%d, entry=%+v", n, entry)
	}

	if err := ft.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !ft.closed {
		t.Error("Close should set closed")
	}
}
