package kkactor

import "time"

type actorContext struct {
	system  *ActorSystem
	self    *PID
	sender  *PID
	message any
	respond chan *FutureResult
}

func (c *actorContext) reset() {
	c.sender = nil
	c.message = nil
	c.respond = nil
}

// Message returns the current message.
func (c *actorContext) Message() any {
	return c.message
}

// Sender returns the sender pid.
func (c *actorContext) Sender() *PID {
	return c.sender
}

// Self returns the current actor pid.
func (c *actorContext) Self() *PID {
	return c.self
}

// System returns the actor system.
func (c *actorContext) System() *ActorSystem {
	return c.system
}

// Respond sends response to the requester.
func (c *actorContext) Respond(msg any) {
	if c.respond == nil {
		return
	}
	select {
	case c.respond <- &FutureResult{Message: msg}:
	default:
	}
}

// Send sends a message to another actor.
func (c *actorContext) Send(pid *PID, msg any) {
	if c == nil || c.system == nil {
		return
	}
	_ = c.system.sendLocal(pid, &Envelope{message: msg, sender: c.self})
}

// RequestFuture sends a message and returns a future for the response.
func (c *actorContext) RequestFuture(pid *PID, msg any, timeout ...time.Duration) *Future {
	if c == nil {
		fut := newFuture()
		fut.complete(&FutureResult{Error: ErrActorDead})
		return fut
	}
	root := &RootContext{system: c.system}
	return root.RequestFuture(pid, msg, timeout...)
}

// Spawn creates a new actor.
func (c *actorContext) Spawn(props *Props) *PID {
	if c == nil || c.system == nil {
		return nil
	}
	return c.system.Spawn(props)
}

// Stop stops an actor.
func (c *actorContext) Stop(pid *PID) {
	if c == nil || c.system == nil {
		return
	}
	c.system.StopPID(pid)
}

// StopFuture stops an actor and returns a future for completion.
func (c *actorContext) StopFuture(pid *PID) *Future {
	if c == nil || c.system == nil {
		fut := newFuture()
		fut.complete(&FutureResult{Error: ErrActorDead})
		return fut
	}
	return c.system.StopFuture(pid)
}
