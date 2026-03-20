package actornats

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/nats-io/nats.go"
	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/kkactor"
	"github.com/vvisun/kkdg/kkapp/kkactor/actorremotes"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

func requireNATS(t *testing.T) string {
	t.Helper()
	url := "nats://127.0.0.1:4222"
	nc, err := nats.Connect(url, nats.Timeout(500*time.Millisecond))
	if err != nil {
		fmt.Printf("NATS not available: %v\n", err)
		t.Skipf("NATS not available: %v", err)
	}
	nc.Close()
	return url
}

type remotePing struct {
	Text string
}

type remotePong struct {
	Text string
}

func getTestMessageRegistry(t *testing.T) *actorremotes.MessageRegistry {
	t.Helper()
	registry := actorremotes.NewMessageRegistry(kkcodec.GetCodec(kkcodec.CodecTypeMsgpack))
	if err := registry.Register(&remotePing{}); err != nil {
		t.Fatalf("register ping on transport1: %v", err)
	}
	if err := registry.Register(&remotePong{}); err != nil {
		t.Fatalf("register pong on transport1: %v", err)
	}
	return registry
}

func TestTransport_SendAndRequest(t *testing.T) {
	natsURL := requireNATS(t)

	registry := getTestMessageRegistry(t)
	transport1 := NewTransport("node1", registry, ApplyNatsOptions(WithURL(natsURL)))
	transport2 := NewTransport("node2", registry, ApplyNatsOptions(WithURL(natsURL)))

	framework1 := kkactor.NewActorFramework()
	framework2 := kkactor.NewActorFramework()
	framework1.GetLocator().AddNode(kkapp.NewNodeInfo("node1", "game", "", ""))
	framework2.GetLocator().AddNode(kkapp.NewNodeInfo("node2", "game", "", ""))

	if err := framework1.SetRemoteTransport(transport1); err != nil {
		t.Fatalf("framework1.SetRemoteTransport: %v", err)
	}
	if err := framework2.SetRemoteTransport(transport2); err != nil {
		t.Fatalf("framework2.SetRemoteTransport: %v", err)
	}
	defer func() {
		_ = transport1.Close()
		_ = transport2.Close()
	}()

	received := make(chan string, 1)
	props := actor.PropsFromFunc(func(ctx actor.Context) {
		switch msg := ctx.Message().(type) {
		case *remotePing:
			received <- msg.Text
			if ctx.Sender() != nil {
				ctx.Respond(&remotePong{Text: "pong:" + msg.Text})
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
	if err := framework1.Send(targetID, &remotePing{Text: "hello"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	select {
	case got := <-received:
		if got != "hello" {
			t.Fatalf("received = %q, want hello", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("remote send timeout")
	}

	result, err := framework1.Request(targetID, &remotePing{Text: "rpc"}, 2*time.Second)
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	rsp, ok := result.(*remotePong)
	if !ok {
		t.Fatalf("result type = %T, want *remotePong", result)
	}
	if rsp.Text != "pong:rpc" {
		t.Fatalf("response = %q, want pong:rpc", rsp.Text)
	}
}

func TestTransport_Request_UnregisteredMessage(t *testing.T) {
	natsURL := requireNATS(t)
	registry := getTestMessageRegistry(t)

	transport := NewTransport("node1", registry, ApplyNatsOptions(WithURL(natsURL)))
	framework := kkactor.NewActorFramework()
	framework.GetLocator().AddNode(kkapp.NewNodeInfo("node1", "game", "", ""))
	if err := framework.SetRemoteTransport(transport); err != nil {
		t.Fatalf("SetRemoteTransport: %v", err)
	}
	defer func() { _ = transport.Close() }()

	targetID, err := kkactor.NewLucencyID("node2", "echo")
	if err != nil {
		t.Fatalf("NewLucencyID: %v", err)
	}
	targetRef := actorremotes.ActorRef{NodeID: "node2", ActorKey: "echo"}
	_, err = framework.Request(targetID, &struct{ Text string }{Text: "x"}, time.Second)
	if err == nil || !errors.Is(err, kkerrors.ErrActorRemoteMsgTypeNotRegistered) {
		t.Fatalf("Request err = %v, want %v", err, kkerrors.ErrActorRemoteMsgTypeNotRegistered)
	}

	if _, err := transport.Request(targetRef, &struct{ Text string }{Text: "x"}, time.Second); err == nil || !errors.Is(err, kkerrors.ErrActorRemoteMsgTypeNotRegistered) {
		t.Fatalf("transport.Request err = %v, want %v", err, kkerrors.ErrActorRemoteMsgTypeNotRegistered)
	}
}
