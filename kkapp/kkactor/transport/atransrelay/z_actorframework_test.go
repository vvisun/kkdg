package atransrelay

import (
	"testing"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp/kkactor"
)

// TestActorFramework_HubRemoteSendAndRequest 验证 ActorFramework + actorshard 中心服
// 与 actornats/z_transport_test.go 中 TestTransport_SendAndRequest 场景一致。
func TestActorFramework_HubRemoteSendAndRequest(t *testing.T) {
	hub := NewHub(freeTCPAddr(t), nil)
	if err := hub.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = hub.Stop() }()

	addr := hub.Addr()
	reg := testRegistry(t)

	framework2 := kkactor.NewActorFramework()
	transport2 := NewTransport("node2", reg, Options{HubAddr: addr, NodeType: "game"})
	if err := framework2.SetRemoteTransport(transport2); err != nil {
		t.Fatalf("framework2.SetRemoteTransport: %v", err)
	}

	framework1 := kkactor.NewActorFramework()
	transport1 := NewTransport("node1", reg, Options{HubAddr: addr})
	if err := framework1.SetRemoteTransport(transport1); err != nil {
		t.Fatalf("framework1.SetRemoteTransport: %v", err)
	}
	defer func() {
		_ = framework1.SetRemoteTransport(nil)
		_ = framework2.SetRemoteTransport(nil)
	}()

	received := make(chan string, 1)
	props := actor.PropsFromFunc(func(ctx actor.Context) {
		switch msg := ctx.Message().(type) {
		case *hpMsg:
			received <- msg.S
			if ctx.Sender() != nil {
				ctx.Respond(&hpMsg{S: "pong:" + msg.S})
			}
		}
	})
	pid := framework2.GetActorSystem().Root.Spawn(props)
	defer framework2.GetActorSystem().Root.Stop(pid)

	targetID, err := kkactor.NewLucencyID("node2", "echo")
	if err != nil {
		t.Fatalf("NewLucencyID: %v", err)
	}
	if err := framework2.GetLocator().AddActor(targetID, pid); err != nil {
		t.Fatalf("AddActor: %v", err)
	}

	if err := framework1.Send(targetID, &hpMsg{S: "hello"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	select {
	case got := <-received:
		if got != "hello" {
			t.Fatalf("received = %q, want hello", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("remote send timeout")
	}

	result, err := framework1.Request(targetID, &hpMsg{S: "rpc"}, 2*time.Second)
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	rsp, ok := result.(*hpMsg)
	if !ok {
		t.Fatalf("result type = %T, want *hpMsg", result)
	}
	if rsp.S != "pong:rpc" {
		t.Fatalf("response = %q, want pong:rpc", rsp.S)
	}
}
