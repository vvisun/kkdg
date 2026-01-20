package kkactor

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/vvisun/kkdg/utils/kklog"
)

// TimerID represents a timer identifier for cancellation.
type TimerID int64

// Actor is the interface that actors must implement.
type Actor interface {
	Receive(ctx IContext)
}

// ActorFunc is a function that implements Actor.
type ActorFunc func(ctx IContext)

// Receive implements Actor.
func (f ActorFunc) Receive(ctx IContext) {
	f(ctx)
}

// Props represents actor properties/configuration.
type Props struct {
	actorProducer func() Actor
}

// PropsFromFunc creates Props from an ActorFunc.
func PropsFromFunc(fn func(ctx IContext)) *Props {
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

// timerInfo represents a timer (one-shot or periodic).
type timerInfo struct {
	id       TimerID
	timer    *time.Timer
	ticker   *time.Ticker
	message  interface{}
	periodic bool
	cancelCh chan struct{}
}

// actorInstance represents a running actor instance.
type actorInstance struct {
	pid         *PID
	actor       Actor
	mailbox     chan envelope
	context     *actorContext
	stopCh      chan struct{}
	doneCh      chan struct{}
	wg          sync.WaitGroup
	stopping    atomic.Bool
	timers      map[TimerID]*timerInfo
	timersMu    sync.Mutex
	nextTimerID atomic.Int64
}

// envelope wraps a message with optional sender information.
type envelope struct {
	msg    interface{}
	sender *PID
}

// scheduleAfter schedules a one-shot timer.
func (inst *actorInstance) scheduleAfter(duration time.Duration, message interface{}) TimerID {
	if inst.stopping.Load() {
		return 0
	}

	id := TimerID(inst.nextTimerID.Add(1))
	timer := time.NewTimer(duration)
	cancelCh := make(chan struct{})

	info := &timerInfo{
		id:       id,
		timer:    timer,
		message:  message,
		periodic: false,
		cancelCh: cancelCh,
	}

	inst.timersMu.Lock()
	inst.timers[id] = info
	inst.timersMu.Unlock()

	go func() {
		select {
		case <-timer.C:
			// 定时器触发，发送消息
			inst.timersMu.Lock()
			delete(inst.timers, id)
			inst.timersMu.Unlock()

			// 发送消息到 actor 的邮箱
			select {
			case inst.mailbox <- envelope{msg: message, sender: nil}:
			case <-inst.stopCh:
			default:
				kklog.Warnf("Actor mailbox full, dropping timer message to %s", inst.pid.id)
			}
		case <-cancelCh:
			// 定时器被取消
			timer.Stop()
			inst.timersMu.Lock()
			delete(inst.timers, id)
			inst.timersMu.Unlock()
		case <-inst.stopCh:
			// Actor 停止
			timer.Stop()
			inst.timersMu.Lock()
			delete(inst.timers, id)
			inst.timersMu.Unlock()
		}
	}()

	return id
}

// scheduleTick schedules a periodic timer.
func (inst *actorInstance) scheduleTick(interval time.Duration, message interface{}) TimerID {
	if inst.stopping.Load() {
		return 0
	}

	id := TimerID(inst.nextTimerID.Add(1))
	ticker := time.NewTicker(interval)
	cancelCh := make(chan struct{})

	info := &timerInfo{
		id:       id,
		ticker:   ticker,
		message:  message,
		periodic: true,
		cancelCh: cancelCh,
	}

	inst.timersMu.Lock()
	inst.timers[id] = info
	inst.timersMu.Unlock()

	go func() {
		for {
			select {
			case <-ticker.C:
				// 定时器触发，发送消息
				select {
				case inst.mailbox <- envelope{msg: message, sender: nil}:
				case <-inst.stopCh:
					return
				case <-cancelCh:
					return
				default:
					kklog.Warnf("Actor mailbox full, dropping tick message to %s", inst.pid.id)
				}
			case <-cancelCh:
				// 定时器被取消
				ticker.Stop()
				inst.timersMu.Lock()
				delete(inst.timers, id)
				inst.timersMu.Unlock()
				return
			case <-inst.stopCh:
				// Actor 停止
				ticker.Stop()
				inst.timersMu.Lock()
				delete(inst.timers, id)
				inst.timersMu.Unlock()
				return
			}
		}
	}()

	return id
}

// cancelTimer cancels a timer.
func (inst *actorInstance) cancelTimer(id TimerID) {
	inst.timersMu.Lock()
	info, exists := inst.timers[id]
	if exists {
		delete(inst.timers, id)
	}
	inst.timersMu.Unlock()

	if !exists {
		return
	}

	// 发送取消信号
	close(info.cancelCh)

	// 停止定时器
	if info.timer != nil {
		info.timer.Stop()
	}
	if info.ticker != nil {
		info.ticker.Stop()
	}
}

// cancelAllTimers cancels all timers.
func (inst *actorInstance) cancelAllTimers() {
	inst.timersMu.Lock()
	timers := make([]*timerInfo, 0, len(inst.timers))
	for _, info := range inst.timers {
		timers = append(timers, info)
	}
	inst.timers = make(map[TimerID]*timerInfo)
	inst.timersMu.Unlock()

	// 取消所有定时器
	for _, info := range timers {
		close(info.cancelCh)
		if info.timer != nil {
			info.timer.Stop()
		}
		if info.ticker != nil {
			info.ticker.Stop()
		}
	}
}

// run runs the actor's message processing loop.
func (inst *actorInstance) run() {
	defer inst.wg.Done()
	defer func() {
		// 清理所有定时器
		inst.cancelAllTimers()
		close(inst.doneCh)
	}()

	for {
		select {
		case <-inst.stopCh:
			return
		case env := <-inst.mailbox:
			inst.context.message = env.msg
			inst.context.sender = env.sender
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
