package tests

import (
	"context"
	"testing"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkactor"
	"github.com/vvisun/kkdg/kknet/kktcp"
)

func TestKKNetIntegrationTCPActor(t *testing.T) {
	addr := freeTCPAddr(t)

	received := make(chan []byte, 1)
	connected := make(chan struct{}, 1)

	props := actor.PropsFromFunc(func(ctx actor.Context) {
		switch msg := ctx.Message().(type) {
		case *kkactor.ActorEvent:
			switch msg.Type {
			case kkactor.ActorEventConnect:
				select {
				case connected <- struct{}{}:
				default:
				}
			case kkactor.ActorEventMessage:
				if msg.Conn != nil {
					_ = msg.Conn.Send(msg.Data)
				}
				select {
				case received <- msg.Data:
				default:
				}
			}
		}
	})

	actorHandler := kkactor.NewActorHandler(props)
	defer actorHandler.Stop(context.Background())

	server := kktcp.NewServer(addr, actorHandler, kknet.WithPoolSize(4))
	if err := server.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer func() { _ = server.Stop() }()

	replyCh := make(chan []byte, 1)
	clientHandler := &testHandler{
		onMessage: func(c kknet.IConn, data []byte) {
			replyCh <- data
		},
	}
	client := kktcp.NewClient(addr, clientHandler)
	if err := client.Connect(); err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer func() { _ = client.Close() }()

	select {
	case <-connected:
	case <-time.After(2 * time.Second):
		t.Fatal("actor connect timeout")
	}

	payload := []byte("integration")
	if err := client.Send(payload); err != nil {
		t.Fatalf("client send: %v", err)
	}

	select {
	case got := <-received:
		if string(got) != string(payload) {
			t.Fatalf("actor received mismatch: %s", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("actor message timeout")
	}

	select {
	case got := <-replyCh:
		if string(got) != string(payload) {
			t.Fatalf("unexpected reply: %s", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("client reply timeout")
	}
}
