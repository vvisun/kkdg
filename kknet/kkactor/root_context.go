package kkactor

import (
	"time"

	"github.com/vvisun/kkdg/utils/kklog"
)

// Root returns the root context for sending messages.
type RootContext struct {
	system *ActorSystem
}

// NewRootContext 创建一个新的 RootContext。
func NewRootContext(sys *ActorSystem) *RootContext {
	if sys == nil {
		return &RootContext{}
	}
	return &RootContext{system: sys}
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
// 支持本地和远程 actor（透明化）。
func (rc *RootContext) Ask(pid *PID, message interface{}, timeout time.Duration) (reply interface{}, ok bool) {
	if rc == nil || rc.system == nil {
		return nil, false
	}

	// 如果是远程 actor，使用远程系统的 Ask
	if pid != nil && pid.IsRemote() {
		return rc.askRemote(pid, message, timeout)
	}

	// 本地 actor，使用 Future 模式
	fut := NewFuture()

	// 临时回复 actor：收到第一条消息就写入 future 并自杀
	props := PropsFromFunc(func(ctx IContext) {
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

// askRemote 向远程 actor 发送 Ask 请求
func (rc *RootContext) askRemote(pid *PID, message interface{}, timeout time.Duration) (reply interface{}, ok bool) {
	remote := rc.system.GetRemoteSystem()
	if remote == nil {
		kklog.Warnf("RootContext askRemote: no remote system configured")
		return nil, false
	}

	remotePID := RemotePID{
		Addr:    pid.nodeID,
		ActorID: pid.id,
	}

	return remote.Ask(remotePID, message, timeout)
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
