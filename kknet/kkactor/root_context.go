package kkactor

import "time"

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

// Ask sends a message to an actor and waits synchronously for a single reply.
// 返回值 ok=false 表示超时或系统为空。
func (rc *RootContext) Ask(pid *PID, message interface{}, timeout time.Duration) (reply interface{}, ok bool) {
	if rc == nil || rc.system == nil {
		return nil, false
	}

	// Future 用于接收一次性回复
	fut := NewFuture()

	// 临时回复 actor：收到第一条消息就写入 future 并自杀
	props := PropsFromFunc(func(ctx Context) {
		select {
		case fut.ch <- ctx.Message():
		default:
		}
		ctx.Stop(ctx.Self())
	})

	replyPID := rc.system.Spawn(props)
	if replyPID == nil {
		return nil, false
	}

	// 使用 replyPID 作为 sender 发送请求
	rc.system.send(pid, message, replyPID)

	return fut.Result(timeout)
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

// Future represents a one-shot result for Ask pattern.
type Future struct {
	ch chan interface{}
}

// NewFuture creates a new future.
func NewFuture() *Future {
	return &Future{
		ch: make(chan interface{}, 1),
	}
}

// Result waits for the result until timeout.
// ok=false 表示超时。
func (f *Future) Result(timeout time.Duration) (result interface{}, ok bool) {
	if f == nil {
		return nil, false
	}
	if timeout <= 0 {
		// no timeout: block until result
		res, ok2 := <-f.ch
		return res, ok2
	}
	select {
	case res, ok2 := <-f.ch:
		return res, ok2
	case <-time.After(timeout):
		return nil, false
	}
}
