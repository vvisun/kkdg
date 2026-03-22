package atransrelay

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kkapp/kkactor/transport/actortrans"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

func freeTCPAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

type hpMsg struct {
	S string
}

type testRecv struct {
	mu   sync.Mutex
	last any
	ch   chan struct{}
}

func (r *testRecv) HandleRemoteSend(_ actortrans.ActorRef, msg any) error {
	r.mu.Lock()
	r.last = msg
	r.mu.Unlock()
	select {
	case r.ch <- struct{}{}:
	default:
	}
	return nil
}

func (r *testRecv) HandleRemoteRequest(_ actortrans.ActorRef, msg any, _ time.Duration) (any, error) {
	if m, ok := msg.(*hpMsg); ok && m.S == "req" {
		return &hpMsg{S: "ack"}, nil
	}
	return nil, nil
}

func testRegistry(t *testing.T) *actortrans.MessageRegistry {
	t.Helper()
	r := actortrans.NewMessageRegistry(kkcodec.GetCodec(kkcodec.CodecTypeJson))
	if err := r.Register(&hpMsg{}); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestHubTransportSend(t *testing.T) {
	hub := NewHub(freeTCPAddr(t))
	if err := hub.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = hub.Stop() }()

	addr := hub.Addr()
	reg := testRegistry(t)
	tr2 := NewTransport("nodeB", reg, Options{HubAddr: addr, NodeType: "test"})
	tr1 := NewTransport("nodeA", reg, Options{HubAddr: addr})
	rcv := &testRecv{ch: make(chan struct{}, 1)}
	tr2.SetReceiver(rcv)

	if err := tr2.Start(); err != nil {
		t.Fatal(err)
	}
	if err := tr1.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = tr1.Close()
		_ = tr2.Close()
	}()

	target := actortrans.ActorRef{NodeID: "nodeB", ActorKey: "actor1"}
	if err := tr1.Send(target, &hpMsg{S: "hi"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-rcv.ch:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for remote send")
	}
	rcv.mu.Lock()
	v := rcv.last
	rcv.mu.Unlock()
	m, ok := v.(*hpMsg)
	if !ok || m.S != "hi" {
		t.Fatalf("got %+v", v)
	}
}

func TestHubTransportRequest(t *testing.T) {
	hub := NewHub(freeTCPAddr(t))
	if err := hub.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = hub.Stop() }()

	addr := hub.Addr()
	reg := testRegistry(t)
	tr2 := NewTransport("nodeB", reg, Options{HubAddr: addr})
	tr1 := NewTransport("nodeA", reg, Options{HubAddr: addr})
	tr2.SetReceiver(&testRecv{ch: make(chan struct{}, 1)})

	if err := tr2.Start(); err != nil {
		t.Fatal(err)
	}
	if err := tr1.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = tr1.Close()
		_ = tr2.Close()
	}()

	target := actortrans.ActorRef{NodeID: "nodeB", ActorKey: "actor1"}
	res, err := tr1.Request(target, &hpMsg{S: "req"}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	out, ok := res.(*hpMsg)
	if !ok || out.S != "ack" {
		t.Fatalf("got %+v", res)
	}
}
