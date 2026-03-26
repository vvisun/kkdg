package atransnats

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/nats-io/nats.go"
	"github.com/vvisun/kkdg/kkapp/kkactor"
	"github.com/vvisun/kkdg/kkapp/kkactor/transport/actortrans"
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

// remoteMutualKickoff 仅用于「双 actor 交互」测试：触发 actor A 向 B 发起异步 Request。
type remoteMutualKickoff struct{}

func getTestMessageRegistry(t *testing.T) *actortrans.MessageRegistry {
	t.Helper()
	registry := actortrans.NewMessageRegistry(kkcodec.GetCodec(kkcodec.CodecTypeJson))
	if err := registry.Register(&remotePing{}); err != nil {
		t.Fatalf("register ping on transport1: %v", err)
	}
	if err := registry.Register(&remotePong{}); err != nil {
		t.Fatalf("register pong on transport1: %v", err)
	}
	if err := registry.Register(&remoteMutualKickoff{}); err != nil {
		t.Fatalf("register mutual kickoff: %v", err)
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
	if err := framework2.GetLocalActorMgr().AddActor(targetID, pid); err != nil {
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

// TestTransport_TwoActorsBidirectionalRequest：node1 上的 actor A 与 node2 上的 actor B 互相可被 Request，
// 验证跨节点双向 RPC 路由。
func TestTransport_TwoActorsBidirectionalRequest(t *testing.T) {
	natsURL := requireNATS(t)

	registry := getTestMessageRegistry(t)
	transport1 := NewTransport("node1", registry, ApplyNatsOptions(WithURL(natsURL)))
	transport2 := NewTransport("node2", registry, ApplyNatsOptions(WithURL(natsURL)))

	framework1 := kkactor.NewActorFramework()
	framework2 := kkactor.NewActorFramework()

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

	idA, err := kkactor.NewLucencyID("node1", "actor-a")
	if err != nil {
		t.Fatalf("NewLucencyID A: %v", err)
	}
	idB, err := kkactor.NewLucencyID("node2", "actor-b")
	if err != nil {
		t.Fatalf("NewLucencyID B: %v", err)
	}

	echoProps := func() *actor.Props {
		return actor.PropsFromFunc(func(ctx actor.Context) {
			switch msg := ctx.Message().(type) {
			case *remotePing:
				if ctx.Sender() != nil {
					ctx.Respond(&remotePong{Text: "pong:" + msg.Text})
				}
			}
		})
	}

	pidA := framework1.GetActorSystem().Root.Spawn(echoProps())
	pidB := framework2.GetActorSystem().Root.Spawn(echoProps())
	defer framework1.GetActorSystem().Root.Stop(pidA)
	defer framework2.GetActorSystem().Root.Stop(pidB)

	if err := framework1.GetLocalActorMgr().AddActor(idA, pidA); err != nil {
		t.Fatalf("AddActor A: %v", err)
	}
	if err := framework2.GetLocalActorMgr().AddActor(idB, pidB); err != nil {
		t.Fatalf("AddActor B: %v", err)
	}

	resAB, err := framework1.Request(idB, &remotePing{Text: "a-to-b"}, 3*time.Second)
	if err != nil {
		t.Fatalf("framework1.Request(B): %v", err)
	}
	if p, ok := resAB.(*remotePong); !ok || p.Text != "pong:a-to-b" {
		t.Fatalf("A->B response = %#v, want pong:a-to-b", resAB)
	}

	resBA, err := framework2.Request(idA, &remotePing{Text: "b-to-a"}, 3*time.Second)
	if err != nil {
		t.Fatalf("framework2.Request(A): %v", err)
	}
	if p, ok := resBA.(*remotePong); !ok || p.Text != "pong:b-to-a" {
		t.Fatalf("B->A response = %#v, want pong:b-to-a", resBA)
	}
}

// TestTransport_TwoActorsNestedRemoteRequest：A 通过 RequestAsync 调用 B（不阻塞 A 的邮箱），
// B 在处理中再同步 Request 回 A，形成跨节点「回调式」双跳 RPC；若 A 对 B 使用同步 Request 会阻塞邮箱导致死锁。
func TestTransport_TwoActorsNestedRemoteRequest(t *testing.T) {
	natsURL := requireNATS(t)

	registry := getTestMessageRegistry(t)
	transport1 := NewTransport("node1", registry, ApplyNatsOptions(WithURL(natsURL)))
	transport2 := NewTransport("node2", registry, ApplyNatsOptions(WithURL(natsURL)))

	framework1 := kkactor.NewActorFramework()
	framework2 := kkactor.NewActorFramework()

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

	idA, err := kkactor.NewLucencyID("node1", "actor-a")
	if err != nil {
		t.Fatalf("NewLucencyID A: %v", err)
	}
	idB, err := kkactor.NewLucencyID("node2", "actor-b")
	if err != nil {
		t.Fatalf("NewLucencyID B: %v", err)
	}

	asyncDone := make(chan string, 1)

	propsA := actor.PropsFromFunc(func(ctx actor.Context) {
		switch msg := ctx.Message().(type) {
		case *remoteMutualKickoff:
			_ = framework1.RequestAsync(idB, &remotePing{Text: "from-a"}, 5*time.Second, func(result any, err error) {
				if err != nil {
					asyncDone <- "callback-err:" + err.Error()
					return
				}
				pong, ok := result.(*remotePong)
				if !ok || pong == nil {
					asyncDone <- fmt.Sprintf("callback-bad-type:%T", result)
					return
				}
				asyncDone <- pong.Text
			})
		case *remotePing:
			ctx.Respond(&remotePong{Text: "a:" + msg.Text})
		}
	})

	propsB := actor.PropsFromFunc(func(ctx actor.Context) {
		switch msg := ctx.Message().(type) {
		case *remotePing:
			sub, err := framework2.Request(idA, &remotePing{Text: "b-nested:" + msg.Text}, 4*time.Second)
			if err != nil {
				if ctx.Sender() != nil {
					ctx.Respond(&remotePong{Text: "b-err:" + err.Error()})
				}
				return
			}
			inner, _ := sub.(*remotePong)
			innerText := ""
			if inner != nil {
				innerText = inner.Text
			}
			if ctx.Sender() != nil {
				ctx.Respond(&remotePong{Text: "b-wraps:" + innerText + ":orig:" + msg.Text})
			}
		}
	})

	pidA := framework1.GetActorSystem().Root.Spawn(propsA)
	pidB := framework2.GetActorSystem().Root.Spawn(propsB)
	defer framework1.GetActorSystem().Root.Stop(pidA)
	defer framework2.GetActorSystem().Root.Stop(pidB)

	if err := framework1.GetLocalActorMgr().AddActor(idA, pidA); err != nil {
		t.Fatalf("AddActor A: %v", err)
	}
	if err := framework2.GetLocalActorMgr().AddActor(idB, pidB); err != nil {
		t.Fatalf("AddActor B: %v", err)
	}

	if err := framework1.Send(idA, &remoteMutualKickoff{}); err != nil {
		t.Fatalf("kickoff Send: %v", err)
	}

	want := "b-wraps:a:b-nested:from-a:orig:from-a"
	select {
	case got := <-asyncDone:
		if got != want {
			t.Fatalf("async result = %q, want %q", got, want)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("nested remote request timeout")
	}
}

func TestTransport_Request_UnregisteredMessage(t *testing.T) {
	natsURL := requireNATS(t)
	registry := getTestMessageRegistry(t)

	transport := NewTransport("node1", registry, ApplyNatsOptions(WithURL(natsURL)))
	framework := kkactor.NewActorFramework()
	if err := framework.SetRemoteTransport(transport); err != nil {
		t.Fatalf("SetRemoteTransport: %v", err)
	}
	defer func() { _ = transport.Close() }()

	targetID, err := kkactor.NewLucencyID("node2", "echo")
	if err != nil {
		t.Fatalf("NewLucencyID: %v", err)
	}
	targetRef := actortrans.ActorRef{NodeID: "node2", ActorKey: "echo"}
	_, err = framework.Request(targetID, &struct{ Text string }{Text: "x"}, time.Second)
	if err == nil || !errors.Is(err, kkerrors.ErrActorRemoteMsgTypeNotRegistered) {
		t.Fatalf("Request err = %v, want %v", err, kkerrors.ErrActorRemoteMsgTypeNotRegistered)
	}

	if _, err := transport.Request(targetRef, &struct{ Text string }{Text: "x"}, time.Second); err == nil || !errors.Is(err, kkerrors.ErrActorRemoteMsgTypeNotRegistered) {
		t.Fatalf("transport.Request err = %v, want %v", err, kkerrors.ErrActorRemoteMsgTypeNotRegistered)
	}
}
