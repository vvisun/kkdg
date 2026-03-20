package kkactor

import (
	"errors"
	"testing"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkerrors"
)

//------------------------------------------------------------------------------
// framework_test
//------------------------------------------------------------------------------

func TestActorFramework_Send_NotFound(t *testing.T) {
	af := NewActorFramework()
	af.GetLocator().AddNode(kkapp.NewNodeInfo("aa", "game", "", ""))

	id, _ := NewLucencyID("aa", "nonexistent")
	err := af.Send(id, "hello")
	if err == nil || !errors.Is(err, kkerrors.ErrActorNotFound) {
		t.Errorf("Send(not found) = %v, want ErrActorNotFound", err)
	}
}

func TestActorFramework_Request_NotFound(t *testing.T) {
	af := NewActorFramework()
	af.GetLocator().AddNode(kkapp.NewNodeInfo("aa", "game", "", ""))

	id, _ := NewLucencyID("aa", "nonexistent")
	_, err := af.Request(id, "hello", time.Second)
	if err == nil || !errors.Is(err, kkerrors.ErrActorNotFound) {
		t.Errorf("Request(not found) = %v, want ErrActorNotFound", err)
	}
}

func TestActorFramework_RequestAsync_NotFound(t *testing.T) {
	af := NewActorFramework()
	af.GetLocator().AddNode(kkapp.NewNodeInfo("aa", "game", "", ""))

	id, _ := NewLucencyID("aa", "nonexistent")
	err := af.RequestAsync(id, "hello", time.Second, func(result any, err error) {})
	if err == nil || !errors.Is(err, kkerrors.ErrActorNotFound) {
		t.Errorf("RequestAsync(not found) = %v, want ErrActorNotFound", err)
	}
}

func TestActorFramework_RequestAsync_NilCallback(t *testing.T) {
	af := NewActorFramework()
	af.GetLocator().AddNode(kkapp.NewNodeInfo("aa", "game", "", ""))
	actorSys := af.GetActorSystem()
	loc := af.GetLocator()

	id, _ := NewLucencyID("aa", "echo")
	pid := actorSys.Root.Spawn(actor.PropsFromFunc(func(ctx actor.Context) {
		if ctx.Sender() != nil {
			ctx.Respond(ctx.Message())
		}
	}))
	defer actorSys.Root.Stop(pid)
	if err := loc.AddActor(id, pid); err != nil {
		t.Fatalf("AddActor: %v", err)
	}

	err := af.RequestAsync(id, "hello", time.Second, nil)
	if err == nil || !errors.Is(err, kkerrors.ErrActorAsyncCallbackNil) {
		t.Fatalf("RequestAsync(nil callback) = %v, want %v", err, kkerrors.ErrActorAsyncCallbackNil)
	}
}

func TestActorFramework_SetRemoteTransport_RollbackOnStartError(t *testing.T) {
	af := NewActorFramework()

	oldTransport := &stubRemoteTransport{}
	if err := af.SetRemoteTransport(oldTransport); err != nil {
		t.Fatalf("SetRemoteTransport(old) = %v", err)
	}
	if af.GetRemoteTransport() != oldTransport {
		t.Fatal("old transport should be installed")
	}

	startErr := errors.New("start failed")
	newTransport := &stubRemoteTransport{startErr: startErr}
	err := af.SetRemoteTransport(newTransport)
	if err == nil || !errors.Is(err, startErr) {
		t.Fatalf("SetRemoteTransport(new) = %v, want %v", err, startErr)
	}
	if af.GetRemoteTransport() != oldTransport {
		t.Fatal("old transport should remain installed after start failure")
	}
	if oldTransport.closeCalls != 0 {
		t.Fatalf("old transport closeCalls = %d, want 0", oldTransport.closeCalls)
	}
	if newTransport.startCalls != 1 {
		t.Fatalf("new transport startCalls = %d, want 1", newTransport.startCalls)
	}
	if newTransport.receiverSet != af {
		t.Fatal("new transport receiver should be set before start")
	}
}

func TestActorFramework_Send_Request_Integration(t *testing.T) {
	af := NewActorFramework()
	actorSys := af.GetActorSystem()
	loc := af.GetLocator()
	af.GetLocator().AddNode(kkapp.NewNodeInfo("aa", "game", "", ""))

	id, err := NewLucencyID("aa", "echo")
	if err != nil {
		t.Fatalf("NewLucencyActorID: %v", err)
	}

	echoProps := actor.PropsFromFunc(func(ctx actor.Context) {
		if ctx.Sender() != nil {
			ctx.Respond(ctx.Message())
		}
	})
	pid := actorSys.Root.Spawn(echoProps)
	defer actorSys.Root.Stop(pid)

	if err := loc.AddActor(id, pid); err != nil {
		t.Fatalf("AddActor: %v", err)
	}

	// Send (fire-and-forget, no response expected)
	if err := af.Send(id, "hello"); err != nil {
		t.Errorf("Send: %v", err)
	}

	// Request (sync)
	type req struct{ V int }
	type rsp struct{ V int }
	reqMsg := &req{V: 42}
	result, err := af.Request(id, reqMsg, time.Second)
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	r, ok := result.(*req)
	if !ok {
		t.Fatalf("Request result type = %T, want *req", result)
	}
	if r.V != 42 {
		t.Errorf("Request result.V = %d, want 42", r.V)
	}
}

func TestActorFramework_RequestAsync_Integration(t *testing.T) {
	af := NewActorFramework()
	actorSys := af.GetActorSystem()
	loc := af.GetLocator()
	af.GetLocator().AddNode(kkapp.NewNodeInfo("aa", "game", "", ""))

	id, _ := NewLucencyID("aa", "echo")
	echoProps := actor.PropsFromFunc(func(ctx actor.Context) {
		if ctx.Sender() != nil {
			ctx.Respond(ctx.Message())
		}
	})
	pid := actorSys.Root.Spawn(echoProps)
	defer actorSys.Root.Stop(pid)
	_ = loc.AddActor(id, pid)

	done := make(chan struct{})
	err := af.RequestAsync(id, &struct{ V int }{V: 99}, time.Second, func(result any, err error) {
		if err != nil {
			t.Errorf("RequestAsync callback err = %v", err)
			close(done)
			return
		}
		r, ok := result.(*struct{ V int })
		if !ok || r.V != 99 {
			t.Errorf("RequestAsync result = %v", result)
		}
		close(done)
	})
	if err != nil {
		t.Fatalf("RequestAsync: %v", err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RequestAsync callback timeout")
	}
}

func TestRequest_RequestAsync_Generic(t *testing.T) {
	af := NewActorFramework()
	actorSys := af.GetActorSystem()
	loc := af.GetLocator()
	af.GetLocator().AddNode(kkapp.NewNodeInfo("aa", "game", "", ""))

	id, _ := NewLucencyID("aa", "echo")
	echoProps := actor.PropsFromFunc(func(ctx actor.Context) {
		if ctx.Sender() != nil {
			ctx.Respond(ctx.Message())
		}
	})
	pid := actorSys.Root.Spawn(echoProps)
	defer actorSys.Root.Stop(pid)
	_ = loc.AddActor(id, pid)

	// Echo returns same type as request
	type Msg struct{ V string }
	req := &Msg{V: "ok"}

	rsp, err := af.Request(id, req, time.Second)
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	r, ok := rsp.(*Msg)
	if !ok || r.V != "ok" {
		t.Errorf("Request Rsp.V = %q, want ok", r.V)
	}

	done := make(chan struct{})
	err = af.RequestAsync(id, req, time.Second, func(result any, err error) {
		if err != nil || result.(*Msg).V != "ok" {
			t.Errorf("RequestAsync result = %v err = %v", result, err)
		}
		close(done)
	})
	if err != nil {
		t.Fatalf("RequestAsync: %v", err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RequestAsync callback timeout")
	}
}
