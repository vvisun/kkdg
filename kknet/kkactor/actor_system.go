package kkactor

import (
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// ActorSystem manages actor lifecycles.
type ActorSystem struct {
	nextID  uint64
	actors  sync.Map // map[string]*actorProcess
	stopped atomic.Bool
	nodeID  string
	remote  Remote
	codec   RemoteCodec
}

// NewActorSystem creates a new system.
func NewActorSystem() *ActorSystem {
	return &ActorSystem{}
}

// Spawn creates and starts an actor.
func (s *ActorSystem) Spawn(props *Props) *PID {
	if s == nil || props == nil || props.Producer == nil {
		return nil
	}
	if props.MailboxSize <= 0 {
		props.MailboxSize = defaultMailboxSize
	}
	id := strconv.FormatUint(atomic.AddUint64(&s.nextID, 1), 10)
	pid := &PID{id: id, system: s, nodeID: s.nodeID}
	process := newActorProcess(s, pid, props)
	s.actors.Store(pid.id, process)
	process.start()
	return pid
}

// Stop stops all actors in the system.
func (s *ActorSystem) Stop() {
	if s == nil || s.stopped.Swap(true) {
		return
	}
	s.actors.Range(func(_, value any) bool {
		process, ok := value.(*actorProcess)
		if ok {
			process.stop(nil)
		}
		return true
	})
}

// StopPID stops a specific actor.
func (s *ActorSystem) StopPID(pid *PID) {
	if s == nil {
		return
	}
	_ = s.StopFuture(pid)
}

// StopFuture stops a specific actor and returns a future.
func (s *ActorSystem) StopFuture(pid *PID) *Future {
	fut := newFuture()
	if s == nil || pid == nil {
		fut.complete(&FutureResult{Error: ErrActorDead})
		return fut
	}
	process, ok := s.actors.Load(pid.id)
	if !ok {
		fut.complete(&FutureResult{Error: ErrActorDead})
		return fut
	}
	if ap, ok := process.(*actorProcess); ok {
		ap.stop(fut)
		return fut
	}
	fut.complete(&FutureResult{Error: ErrActorDead})
	return fut
}

func (s *ActorSystem) sendLocal(pid *PID, env *Envelope) error {
	if s == nil || pid == nil {
		return ErrActorDead
	}
	if s.isRemote(pid) {
		return s.sendRemote(pid, env)
	}
	process, ok := s.actors.Load(pid.id)
	if !ok {
		return ErrActorDead
	}
	ap, ok := process.(*actorProcess)
	if !ok {
		return ErrActorDead
	}
	return ap.send(env)
}

type Envelope struct {
	message any
	sender  *PID
	respond chan *FutureResult
	timeout time.Duration
}

type stopMessage struct {
	future *Future
}

type actorProcess struct {
	system  *ActorSystem
	pid     *PID
	props   *Props
	mailbox chan *Envelope
	stopped chan struct{}
	actor   Actor
}

func newActorProcess(system *ActorSystem, pid *PID, props *Props) *actorProcess {
	actor := props.Producer()
	if actor == nil {
		actor = noopActor{}
	}
	return &actorProcess{
		system:  system,
		pid:     pid,
		props:   props,
		mailbox: make(chan *Envelope, props.MailboxSize),
		stopped: make(chan struct{}),
		actor:   actor,
	}
}

func (p *actorProcess) start() {
	go p.run()
}

func (p *actorProcess) send(env *Envelope) error {
	if p == nil {
		return ErrActorDead
	}
	select {
	case <-p.stopped:
		return ErrActorDead
	case p.mailbox <- env:
		return nil
	}
}

func (p *actorProcess) stop(fut *Future) {
	if p == nil {
		if fut != nil {
			fut.complete(&FutureResult{Error: ErrActorDead})
		}
		return
	}
	_ = p.send(&Envelope{message: stopMessage{future: fut}})
}

func (p *actorProcess) run() {
	ctx := &actorContext{
		system: p.system,
		self:   p.pid,
	}
	for {
		env := <-p.mailbox
		if env == nil {
			continue
		}
		switch msg := env.message.(type) {
		case stopMessage:
			if msg.future != nil {
				msg.future.complete(&FutureResult{})
			}
			close(p.stopped)
			p.system.actors.Delete(p.pid.id)
			return
		default:
			ctx.sender = env.sender
			ctx.message = env.message
			ctx.respond = env.respond
			p.actor.Receive(ctx)
			ctx.reset()
		}
	}
}

type noopActor struct{}

func (noopActor) Receive(ctx Context) {}
