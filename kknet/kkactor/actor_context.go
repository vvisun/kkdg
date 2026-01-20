package kkactor

import "time"

// IContext represents the actor context for receiving messages.
type IContext interface {
	Message() interface{}
	Self() *PID
	Sender() *PID
	Send(pid *PID, message interface{})
	Stop(pid *PID)
	IContextTimer // timer interface
}

type IContextTimer interface {
	// After schedules a message to be sent after the specified duration.
	// Returns a TimerID that can be used to cancel the timer.
	After(duration time.Duration, message interface{}) TimerID
	// Tick schedules a periodic message to be sent at the specified interval.
	// Returns a TimerID that can be used to cancel the timer.
	Tick(interval time.Duration, message interface{}) TimerID
	// CancelTimer cancels a timer identified by the TimerID.
	CancelTimer(id TimerID)
}

// actorContext implements Context interface.
type actorContext struct {
	instance *actorInstance
	message  interface{}
	sender   *PID
}

var _ IContext = (*actorContext)(nil)

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
	// 通过当前 actor 作为 sender 发送消息
	pid.system.send(pid, message, ctx.Self())
}

func (ctx *actorContext) Stop(pid *PID) {
	if pid == nil || pid.system == nil {
		return
	}
	pid.system.stop(pid)
}

func (ctx *actorContext) After(duration time.Duration, message interface{}) TimerID {
	if ctx.instance == nil {
		return 0
	}
	return ctx.instance.scheduleAfter(duration, message)
}

func (ctx *actorContext) Tick(interval time.Duration, message interface{}) TimerID {
	if ctx.instance == nil {
		return 0
	}
	return ctx.instance.scheduleTick(interval, message)
}

func (ctx *actorContext) CancelTimer(id TimerID) {
	if ctx.instance == nil {
		return
	}
	ctx.instance.cancelTimer(id)
}
