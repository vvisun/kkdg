package atransrelay

import (
	"fmt"
	"testing"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp/kkactor"
	"github.com/vvisun/kkdg/kkapp/kkactor/transport/actortrans"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

type relayRemotePing struct {
	Text string
}

type relayRemotePong struct {
	Text string
}

// relayRemoteMutualKickoff 触发 actor A 向 B 发起异步 Request（与 atransnats 中 remoteMutualKickoff 场景对齐）。
type relayRemoteMutualKickoff struct{}

func twoActorMessageRegistry(t *testing.T) *actortrans.MessageRegistry {
	t.Helper()
	registry := actortrans.NewMessageRegistry(kkcodec.GetCodec(kkcodec.CodecTypeJson))
	if err := registry.Register(&relayRemotePing{}); err != nil {
		t.Fatalf("register ping: %v", err)
	}
	if err := registry.Register(&relayRemotePong{}); err != nil {
		t.Fatalf("register pong: %v", err)
	}
	if err := registry.Register(&relayRemoteMutualKickoff{}); err != nil {
		t.Fatalf("register mutual kickoff: %v", err)
	}
	return registry
}

// TestRelay_TwoActorsBidirectionalRequest：Hub 场景下 node1 的 actor A 与 node2 的 actor B 双向 Request。
func TestRelay_TwoActorsBidirectionalRequest(t *testing.T) {
	hub := NewHub(freeTCPAddr(t))
	if err := hub.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = hub.Stop() }()

	addr := hub.Addr()
	reg := twoActorMessageRegistry(t)

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
			case *relayRemotePing:
				if ctx.Sender() != nil {
					ctx.Respond(&relayRemotePong{Text: "pong:" + msg.Text})
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

	resAB, err := framework1.Request(idB, &relayRemotePing{Text: "a-to-b"}, 3*time.Second)
	if err != nil {
		t.Fatalf("framework1.Request(B): %v", err)
	}
	if p, ok := resAB.(*relayRemotePong); !ok || p.Text != "pong:a-to-b" {
		t.Fatalf("A->B response = %#v, want pong:a-to-b", resAB)
	}

	resBA, err := framework2.Request(idA, &relayRemotePing{Text: "b-to-a"}, 3*time.Second)
	if err != nil {
		t.Fatalf("framework2.Request(A): %v", err)
	}
	if p, ok := resBA.(*relayRemotePong); !ok || p.Text != "pong:b-to-a" {
		t.Fatalf("B->A response = %#v, want pong:b-to-a", resBA)
	}
}

// TestRelay_TwoActorsNestedRemoteRequest：A 对 B 使用 RequestAsync，B 再同步 Request 回 A（与 atransnats 嵌套场景对齐）。
func TestRelay_TwoActorsNestedRemoteRequest(t *testing.T) {
	hub := NewHub(freeTCPAddr(t))
	if err := hub.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = hub.Stop() }()

	addr := hub.Addr()
	reg := twoActorMessageRegistry(t)

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
		case *relayRemoteMutualKickoff:
			_ = framework1.RequestAsync(idB, &relayRemotePing{Text: "from-a"}, 5*time.Second, func(result any, err error) {
				if err != nil {
					asyncDone <- "callback-err:" + err.Error()
					return
				}
				pong, ok := result.(*relayRemotePong)
				if !ok || pong == nil {
					asyncDone <- fmt.Sprintf("callback-bad-type:%T", result)
					return
				}
				asyncDone <- pong.Text
			})
		case *relayRemotePing:
			ctx.Respond(&relayRemotePong{Text: "a:" + msg.Text})
		}
	})

	propsB := actor.PropsFromFunc(func(ctx actor.Context) {
		switch msg := ctx.Message().(type) {
		case *relayRemotePing:
			sub, err := framework2.Request(idA, &relayRemotePing{Text: "b-nested:" + msg.Text}, 4*time.Second)
			if err != nil {
				if ctx.Sender() != nil {
					ctx.Respond(&relayRemotePong{Text: "b-err:" + err.Error()})
				}
				return
			}
			inner, _ := sub.(*relayRemotePong)
			innerText := ""
			if inner != nil {
				innerText = inner.Text
			}
			if ctx.Sender() != nil {
				ctx.Respond(&relayRemotePong{Text: "b-wraps:" + innerText + ":orig:" + msg.Text})
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

	if err := framework1.Send(idA, &relayRemoteMutualKickoff{}); err != nil {
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
