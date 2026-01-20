package kkactor

import "time"

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
