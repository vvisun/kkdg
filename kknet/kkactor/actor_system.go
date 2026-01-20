package kkactor

import (
	"encoding/json"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/utils/kklog"
)

// ActorSystem manages actors.
type ActorSystem struct {
	mu           sync.RWMutex
	actors       map[string]*actorInstance
	nextID       atomic.Uint64
	stopCh       chan struct{}
	stopped      atomic.Bool
	remoteSystem *ClusterRemoteSystem // 远程通信系统（可选）
	nodeID       string               // 当前节点ID
}

// NewActorSystem creates a new actor system.
func NewActorSystem() *ActorSystem {
	return &ActorSystem{
		actors: make(map[string]*actorInstance),
		stopCh: make(chan struct{}),
	}
}

// SetRemoteSystem 设置远程通信系统，启用透明化远程通信
func (sys *ActorSystem) SetRemoteSystem(remote *ClusterRemoteSystem) {
	if sys == nil {
		return
	}
	sys.mu.Lock()
	defer sys.mu.Unlock()
	sys.remoteSystem = remote
	if remote != nil {
		sys.nodeID = remote.GetNodeID()
	}
}

// GetRemoteSystem 获取远程通信系统
func (sys *ActorSystem) GetRemoteSystem() *ClusterRemoteSystem {
	if sys == nil {
		return nil
	}
	sys.mu.RLock()
	defer sys.mu.RUnlock()
	return sys.remoteSystem
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
		nodeID: "", // 本地 actor，nodeID 为空
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
// 自动判断本地/远程并路由。
func (sys *ActorSystem) send(pid *PID, message interface{}, sender *PID) {
	if pid == nil {
		return
	}

	// 如果是远程 actor，通过远程系统发送
	if pid.IsRemote() {
		sys.sendRemote(pid, message, sender)
		return
	}

	// 本地 actor，检查是否属于当前系统
	if pid.system != sys {
		kklog.Warnf("ActorSystem send: PID %s belongs to different system", pid.id)
		return
	}

	sys.sendByID(pid.id, message, sender)
}

// sendRemote 发送消息到远程 actor
func (sys *ActorSystem) sendRemote(pid *PID, message interface{}, sender *PID) {
	sys.mu.RLock()
	remote := sys.remoteSystem
	sys.mu.RUnlock()

	if remote == nil {
		kklog.Warnf("ActorSystem sendRemote: no remote system configured, cannot send to %s", pid.String())
		return
	}

	// 创建远程 PID
	remotePID := RemotePID{
		Addr:    pid.nodeID,
		ActorID: pid.id,
	}

	// 如果消息已经是 []byte，直接使用 TellBytes，避免 JSON 序列化
	if msgData, ok := message.([]byte); ok {
		if err := remote.TellBytes(remotePID, msgData); err != nil {
			kklog.Errorf("ActorSystem sendRemote failed: %v", err)
		}
		return
	}

	// 其他类型序列化为 JSON
	msgData, err := json.Marshal(message)
	if err != nil {
		kklog.Errorf("ActorSystem sendRemote marshal failed: %v", err)
		return
	}

	// 通过远程系统发送
	if err := remote.TellBytes(remotePID, msgData); err != nil {
		kklog.Errorf("ActorSystem sendRemote failed: %v", err)
	}
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
