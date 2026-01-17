package kknet

import (
	"context"

	"github.com/asynkron/protoactor-go/actor"
)

// ActorEventType represents connection event type.
type ActorEventType int

const (
	ActorEventConnect ActorEventType = iota
	ActorEventMessage
	ActorEventClose
)

// ActorEvent is delivered to protoactor actors.
type ActorEvent struct {
	Type ActorEventType
	Conn Conn
	Data []byte
	Err  error
}

// ActorHandler adapts a protoactor actor to a kknet Handler.
type ActorHandler struct {
	system *actor.ActorSystem
	root   *actor.RootContext
	pid    *actor.PID
}

// NewActorHandler creates a handler that forwards events to the actor.
func NewActorHandler(props *actor.Props) *ActorHandler {
	sys := actor.NewActorSystem()
	root := sys.Root
	pid := root.Spawn(props)
	return &ActorHandler{
		system: sys,
		root:   root,
		pid:    pid,
	}
}

// System returns the underlying actor system.
func (h *ActorHandler) System() *actor.ActorSystem {
	return h.system
}

// Stop stops the actor.
func (h *ActorHandler) Stop(ctx context.Context) {
	if h == nil || h.root == nil || h.pid == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	fut := h.root.StopFuture(h.pid)
	done := make(chan error, 1)
	go func() {
		done <- fut.Wait()
	}()
	select {
	case <-ctx.Done():
	case <-done:
	}
}

// OnConnect implements Handler.
func (h *ActorHandler) OnConnect(c Conn) {
	if h == nil {
		return
	}
	h.root.Send(h.pid, &ActorEvent{
		Type: ActorEventConnect,
		Conn: c,
	})
}

// OnMessage implements Handler.
func (h *ActorHandler) OnMessage(c Conn, data []byte) {
	if h == nil {
		return
	}
	h.root.Send(h.pid, &ActorEvent{
		Type: ActorEventMessage,
		Conn: c,
		Data: data,
	})
}

// OnClose implements Handler.
func (h *ActorHandler) OnClose(c Conn, err error) {
	if h == nil {
		return
	}
	h.root.Send(h.pid, &ActorEvent{
		Type: ActorEventClose,
		Conn: c,
		Err:  err,
	})
}
