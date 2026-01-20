package kkactor

import (
	"sync"
	"time"
)

// RootContext allows interaction with actors from the outside.
type RootContext struct {
	system *ActorSystem
}

// NewRootContext creates a root context for the system.
func NewRootContext(system *ActorSystem) *RootContext {
	return &RootContext{system: system}
}

// System returns the actor system.
func (r *RootContext) System() *ActorSystem {
	if r == nil {
		return nil
	}
	return r.system
}

// Spawn creates a new actor.
func (r *RootContext) Spawn(props *Props) *PID {
	if r == nil || r.system == nil {
		return nil
	}
	return r.system.Spawn(props)
}

// Send sends a message without expecting a response.
func (r *RootContext) Send(pid *PID, msg any) {
	if r == nil {
		return
	}
	system := r.system
	if pid != nil && pid.system != nil {
		system = pid.system
	}
	if system == nil {
		return
	}
	_ = system.sendLocal(pid, &Envelope{message: msg})
}

// RequestFuture sends a message and returns a future for the response.
func (r *RootContext) RequestFuture(pid *PID, msg any, timeout ...time.Duration) *Future {
	fut := newFuture()
	if r == nil {
		fut.complete(&FutureResult{Error: ErrActorDead})
		return fut
	}
	system := r.system
	if pid != nil && pid.system != nil {
		system = pid.system
	}
	if system == nil {
		fut.complete(&FutureResult{Error: ErrActorDead})
		return fut
	}
	respond := make(chan *FutureResult, 1)
	var waitTimeout time.Duration
	if len(timeout) > 0 && timeout[0] > 0 {
		waitTimeout = timeout[0]
	}
	if err := system.sendLocal(pid, &Envelope{
		message: msg,
		respond: respond,
		timeout: waitTimeout,
	}); err != nil {
		fut.complete(&FutureResult{Error: err})
		return fut
	}
	go func() {
		if waitTimeout > 0 {
			select {
			case res := <-respond:
				fut.complete(res)
			case <-time.After(waitTimeout):
				fut.complete(&FutureResult{Error: ErrTimeout})
			}
			return
		}
		res := <-respond
		fut.complete(res)
	}()
	return fut
}

// Stop stops the actor.
func (r *RootContext) Stop(pid *PID) {
	if r == nil || r.system == nil {
		return
	}
	r.system.StopPID(pid)
}

// StopFuture stops the actor and returns a future for completion.
func (r *RootContext) StopFuture(pid *PID) *Future {
	if r == nil || r.system == nil {
		fut := newFuture()
		fut.complete(&FutureResult{Error: ErrActorDead})
		return fut
	}
	return r.system.StopFuture(pid)
}

// FutureResult represents a request result.
type FutureResult struct {
	Message any
	Error   error
}

// Future waits for an async result.
type Future struct {
	once   sync.Once
	done   chan struct{}
	result *FutureResult
}

func newFuture() *Future {
	return &Future{done: make(chan struct{})}
}

func (f *Future) complete(result *FutureResult) {
	if f == nil {
		return
	}
	f.once.Do(func() {
		f.result = result
		close(f.done)
	})
}

// Result waits and returns the response.
func (f *Future) Result() (any, error) {
	if f == nil {
		return nil, ErrActorDead
	}
	<-f.done
	if f.result == nil {
		return nil, nil
	}
	return f.result.Message, f.result.Error
}

// Wait waits for completion and returns error only.
func (f *Future) Wait() error {
	_, err := f.Result()
	return err
}
