package kkactor

import (
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/utils/kklog"
)

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
		mailbox: make(chan envelope, 100), // 缓冲通道，避免阻塞
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
		timers:  make(map[TimerID]*timerInfo),
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

	sys.sendByID(pid.id, message, sender)
}

// sendByID 根据 actor ID 发送消息（用于远程消息等场景）。
func (sys *ActorSystem) sendByID(id string, message interface{}, sender *PID) {
	if id == "" {
		return
	}

	sys.mu.RLock()
	instance, exists := sys.actors[id]
	sys.mu.RUnlock()

	if !exists {
		return
	}

	// 非阻塞发送，如果邮箱满了就丢弃消息
	select {
	case instance.mailbox <- envelope{msg: message, sender: sender}:
	default:
		kklog.Warnf("Actor mailbox full, dropping message to %s", id)
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
