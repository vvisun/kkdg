package kkactor

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

type testActor struct {
	received chan int
}

func (a *testActor) Receive(ctx Context) {
	switch msg := ctx.Message().(type) {
	case string:
		if msg == "ping" {
			ctx.Respond("pong")
		}
	case int:
		if a.received != nil {
			a.received <- msg
		}
	}
}

func TestActorSystem_LocalSendRequestStop(t *testing.T) {
	received := make(chan int, 1)
	sys := NewActorSystem()
	root := NewRootContext(sys)
	pid := sys.Spawn(FromProducer(func() Actor {
		return &testActor{received: received}
	}))
	if pid == nil {
		t.Fatalf("spawn returned nil pid")
	}

	root.Send(pid, 42)
	select {
	case value := <-received:
		if value != 42 {
			t.Fatalf("unexpected value: %d", value)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for send")
	}

	fut := root.RequestFuture(pid, "ping", time.Second)
	msg, err := fut.Result()
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if msg != "pong" {
		t.Fatalf("unexpected response: %v", msg)
	}

	stopFut := root.StopFuture(pid)
	if err := stopFut.Wait(); err != nil {
		t.Fatalf("stop failed: %v", err)
	}
}

type mockRemote struct {
	nodeID         string
	publishHandler func(sourceNodeID string, data []byte)
	requestHandler func(sourceNodeID string, data []byte) ([]byte, error)
	peer           *mockRemote
}

func (r *mockRemote) NodeID() string {
	if r == nil {
		return ""
	}
	return r.nodeID
}

func (r *mockRemote) Publish(nodeID string, data []byte) error {
	if r == nil || r.peer == nil {
		return ErrRemoteNotConfigured
	}
	if nodeID != r.peer.nodeID {
		return fmt.Errorf("unknown node: %s", nodeID)
	}
	if r.peer.publishHandler != nil {
		r.peer.publishHandler(r.nodeID, data)
	}
	return nil
}

func (r *mockRemote) Request(nodeID string, data []byte, timeout ...time.Duration) ([]byte, error) {
	if r == nil || r.peer == nil {
		return nil, ErrRemoteNotConfigured
	}
	if nodeID != r.peer.nodeID {
		return nil, fmt.Errorf("unknown node: %s", nodeID)
	}
	if r.peer.requestHandler == nil {
		return nil, errors.New("no handler")
	}
	return r.peer.requestHandler(r.nodeID, data)
}

func (r *mockRemote) SetPublishHandler(handler func(sourceNodeID string, data []byte)) {
	if r == nil {
		return
	}
	r.publishHandler = handler
}

func (r *mockRemote) SetRequestHandler(handler func(sourceNodeID string, data []byte) ([]byte, error)) {
	if r == nil {
		return
	}
	r.requestHandler = handler
}

func TestActorSystem_RemotePublishRequest(t *testing.T) {
	type ping struct {
		Value string
	}
	type pong struct {
		Value string
	}

	RegisterMessageType("kkactor.test.ping."+t.Name(), &ping{})
	RegisterMessageType("kkactor.test.pong."+t.Name(), &pong{})

	remoteA := &mockRemote{nodeID: "nodeA"}
	remoteB := &mockRemote{nodeID: "nodeB"}
	remoteA.peer = remoteB
	remoteB.peer = remoteA

	sysA := NewActorSystem()
	sysA.EnableRemote("nodeA", remoteA, nil)
	sysB := NewActorSystem()
	sysB.EnableRemote("nodeB", remoteB, nil)

	received := make(chan string, 1)
	pidB := sysB.Spawn(FromProducer(func() Actor {
		return ActorFunc(func(ctx Context) {
			switch msg := ctx.Message().(type) {
			case *ping:
				if msg.Value != "" {
					received <- msg.Value
				}
				ctx.Respond(&pong{Value: msg.Value})
			}
		})
	}))

	if pidB == nil {
		t.Fatalf("spawn returned nil pid")
	}

	rootA := NewRootContext(sysA)
	remotePID := NewRemotePID("nodeB", pidB.ID())

	rootA.Send(remotePID, &ping{Value: "pub"})
	select {
	case value := <-received:
		if value != "pub" {
			t.Fatalf("unexpected publish value: %s", value)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for publish")
	}

	fut := rootA.RequestFuture(remotePID, &ping{Value: "req"}, time.Second)
	msg, err := fut.Result()
	if err != nil {
		t.Fatalf("remote request failed: %v", err)
	}
	resp, ok := msg.(*pong)
	if !ok {
		t.Fatalf("unexpected response type: %T", msg)
	}
	if resp.Value != "req" {
		t.Fatalf("unexpected response value: %s", resp.Value)
	}
}

type ActorFunc func(ctx Context)

func (f ActorFunc) Receive(ctx Context) {
	f(ctx)
}
