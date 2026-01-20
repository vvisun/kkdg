package actorhandler

/*



import (
	"context"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkactor"
	"github.com/vvisun/kkdg/utils/buffers"
)


// ActorEventType represents connection event type.
type ActorEventType int

const (
	ActorEventConnect ActorEventType = iota
	ActorEventMessage
	ActorEventClose
)

// ActorEvent is delivered to actors.
type ActorEvent struct {
	Type ActorEventType
	Conn kknet.IConn
	Data []byte
	Err  error
}

// ActorHandler adapts an actor to a kknet Handler.
type ActorHandler struct {
	system *kkactor.ActorSystem
	root   *kkactor.RootContext
	pid    *kkactor.PID
}

// NewActorHandler creates a handler that forwards events to the actor.
func NewActorHandler(props *kkactor.Props) *ActorHandler {
	sys := kkactor.NewActorSystem()
	root := &kkactor.RootContext{system: sys}
	pid := sys.Spawn(props)
	return &ActorHandler{
		system: sys,
		root:   root,
		pid:    pid,
	}
}

// System returns the underlying actor system.
func (h *ActorHandler) System() *kkactor.ActorSystem {
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
	// 停止整个系统
	h.system.Stop()
}

// OnConnect implements Handler.
func (h *ActorHandler) OnConnect(c kknet.IConn) {
	if h == nil {
		return
	}
	h.root.Send(h.pid, &kkactor.ActorEvent{
		Type: kkactor.ActorEventConnect,
		Conn: c,
	})
}

// OnMessage implements Handler.
func (h *ActorHandler) OnMessage(c kknet.IConn, data buffers.IBuffer) {
	if h == nil {
		return
	}
	var payload []byte = append([]byte(nil), data.B...)
	h.root.Send(h.pid, &kkactor.ActorEvent{
		Type: kkactor.ActorEventMessage,
		Conn: c,
		Data: payload,
	})
}

// OnClose implements Handler.
func (h *ActorHandler) OnClose(c kknet.IConn, err error) {
	if h == nil {
		return
	}
	h.root.Send(h.pid, &kkactor.ActorEvent{
		Type: kkactor.ActorEventClose,
		Conn: c,
		Err:  err,
	})
}



*/
