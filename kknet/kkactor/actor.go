package kkactor

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/kklog"
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

// Context represents the actor context for receiving messages.
type Context interface {
	Message() interface{}
	Self() *PID
	Sender() *PID
	Send(pid *PID, message interface{})
	Stop(pid *PID)
}

// Actor is the interface that actors must implement.
type Actor interface {
	Receive(ctx Context)
}

// ActorFunc is a function that implements Actor.
type ActorFunc func(ctx Context)

// Receive implements Actor.
func (f ActorFunc) Receive(ctx Context) {
	f(ctx)
}

// PID represents a process ID (actor identifier).
type PID struct {
	id     string
	system *ActorSystem
}

// String returns the string representation of the PID.
func (pid *PID) String() string {
	return pid.id
}

// Props represents actor properties/configuration.
type Props struct {
	actorProducer func() Actor
}

// PropsFromFunc creates Props from an ActorFunc.
func PropsFromFunc(fn func(ctx Context)) *Props {
	return &Props{
		actorProducer: func() Actor {
			return ActorFunc(fn)
		},
	}
}

// PropsFromProducer creates Props from an actor producer function.
func PropsFromProducer(producer func() Actor) *Props {
	return &Props{
		actorProducer: producer,
	}
}

// actorInstance represents a running actor instance.
type actorInstance struct {
	pid      *PID
	actor    Actor
	mailbox  chan interface{}
	context  *actorContext
	stopCh   chan struct{}
	doneCh   chan struct{}
	wg       sync.WaitGroup
	stopping atomic.Bool
}

// actorContext implements Context interface.
type actorContext struct {
	instance *actorInstance
	message  interface{}
	sender   *PID
}

func (ctx *actorContext) Message() interface{} {
	return ctx.message
}

func (ctx *actorContext) Self() *PID {
	return ctx.instance.pid
}

func (ctx *actorContext) Sender() *PID {
	return ctx.sender
}

func (ctx *actorContext) Send(pid *PID, message interface{}) {
	if pid == nil || pid.system == nil {
		return
	}
	pid.system.send(pid, message, nil)
}

func (ctx *actorContext) Stop(pid *PID) {
	if pid == nil || pid.system == nil {
		return
	}
	pid.system.stop(pid)
}

// ActorSystem manages actors.
type ActorSystem struct {
	mu      sync.RWMutex
	actors  map[string]*actorInstance
	nextID  atomic.Uint64
	stopCh  chan struct{}
	stopped atomic.Bool
}

// NewActorSystem creates a new actor system.
func NewActorSystem() *ActorSystem {
	return &ActorSystem{
		actors: make(map[string]*actorInstance),
		stopCh: make(chan struct{}),
	}
}

// Spawn creates a new actor and returns its PID.
func (sys *ActorSystem) Spawn(props *Props) *PID {
	if props == nil || props.actorProducer == nil {
		return nil
	}

	id := sys.nextID.Add(1)
	pidID := "actor-" + strconv.FormatUint(id, 10)
	pid := &PID{
		id:     pidID,
		system: sys,
	}

	instance := &actorInstance{
		pid:     pid,
		actor:   props.actorProducer(),
		mailbox: make(chan interface{}, 100), // 缓冲通道，避免阻塞
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
	}

	instance.context = &actorContext{
		instance: instance,
	}

	sys.mu.Lock()
	sys.actors[pidID] = instance
	sys.mu.Unlock()

	// 启动 actor 的消息处理循环
	instance.wg.Add(1)
	go instance.run()

	return pid
}

// send sends a message to an actor.
func (sys *ActorSystem) send(pid *PID, message interface{}, sender *PID) {
	if pid == nil || pid.system != sys {
		return
	}

	sys.mu.RLock()
	instance, exists := sys.actors[pid.id]
	sys.mu.RUnlock()

	if !exists {
		return
	}

	// 非阻塞发送，如果邮箱满了就丢弃消息
	select {
	case instance.mailbox <- message:
	default:
		kklog.Warnf("Actor mailbox full, dropping message to %s", pid.id)
	}
}

// stop stops an actor.
func (sys *ActorSystem) stop(pid *PID) {
	if pid == nil || pid.system != sys {
		return
	}

	sys.mu.Lock()
	instance, exists := sys.actors[pid.id]
	if exists {
		delete(sys.actors, pid.id)
	}
	sys.mu.Unlock()

	if !exists {
		return
	}

	if instance.stopping.Swap(true) {
		return
	}

	close(instance.stopCh)
	instance.wg.Wait()
}

// Stop stops all actors and the system.
func (sys *ActorSystem) Stop() {
	if sys.stopped.Swap(true) {
		return
	}

	close(sys.stopCh)

	sys.mu.Lock()
	instances := make([]*actorInstance, 0, len(sys.actors))
	for _, instance := range sys.actors {
		instances = append(instances, instance)
	}
	sys.mu.Unlock()

	for _, instance := range instances {
		if instance.stopping.Swap(true) {
			continue
		}
		close(instance.stopCh)
	}

	for _, instance := range instances {
		instance.wg.Wait()
	}
}

// Root returns the root context for sending messages.
type RootContext struct {
	system *ActorSystem
}

// Send sends a message to an actor.
func (rc *RootContext) Send(pid *PID, message interface{}) {
	if rc == nil || rc.system == nil {
		return
	}
	rc.system.send(pid, message, nil)
}

// StopFuture stops an actor and returns a future that completes when the actor is stopped.
func (rc *RootContext) StopFuture(pid *PID) *StopFuture {
	if rc == nil || rc.system == nil {
		return &StopFuture{done: make(chan error, 1)}
	}

	future := &StopFuture{
		done: make(chan error, 1),
	}

	go func() {
		rc.system.stop(pid)
		future.done <- nil
	}()

	return future
}

// StopFuture represents a future for actor stop operation.
type StopFuture struct {
	done chan error
}

// Wait waits for the stop operation to complete.
func (f *StopFuture) Wait() error {
	if f == nil {
		return nil
	}
	return <-f.done
}

// run runs the actor's message processing loop.
func (inst *actorInstance) run() {
	defer inst.wg.Done()
	defer close(inst.doneCh)

	for {
		select {
		case <-inst.stopCh:
			return
		case msg := <-inst.mailbox:
			inst.context.message = msg
			inst.context.sender = nil // 可以扩展支持 sender
			func() {
				defer func() {
					if r := recover(); r != nil {
						kklog.Errorf("Actor panic in %s: %v", inst.pid.id, r)
					}
				}()
				inst.actor.Receive(inst.context)
			}()
		}
	}
}

// ActorHandler adapts an actor to a kknet Handler.
type ActorHandler struct {
	system *ActorSystem
	root   *RootContext
	pid    *PID
}

// NewActorHandler creates a handler that forwards events to the actor.
func NewActorHandler(props *Props) *ActorHandler {
	sys := NewActorSystem()
	root := &RootContext{system: sys}
	pid := sys.Spawn(props)
	return &ActorHandler{
		system: sys,
		root:   root,
		pid:    pid,
	}
}

// System returns the underlying actor system.
func (h *ActorHandler) System() *ActorSystem {
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
	h.root.Send(h.pid, &ActorEvent{
		Type: ActorEventConnect,
		Conn: c,
	})
}

// OnMessage implements Handler.
func (h *ActorHandler) OnMessage(c kknet.IConn, data buffers.IBuffer) {
	if h == nil {
		return
	}
	var payload []byte = append([]byte(nil), data.B...)
	h.root.Send(h.pid, &ActorEvent{
		Type: ActorEventMessage,
		Conn: c,
		Data: payload,
	})
}

// OnClose implements Handler.
func (h *ActorHandler) OnClose(c kknet.IConn, err error) {
	if h == nil {
		return
	}
	h.root.Send(h.pid, &ActorEvent{
		Type: ActorEventClose,
		Conn: c,
		Err:  err,
	})
}
