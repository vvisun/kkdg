package kkactor

// PID represents a process ID (actor identifier).
// 支持本地和远程 actor，透明化通信。
type PID struct {
	id     string // actor ID
	nodeID string // 节点ID，空字符串表示本地 actor
	system *ActorSystem
}

// String returns the string representation of the PID.
func (pid *PID) String() string {
	if pid == nil {
		return ""
	}
	if pid.nodeID != "" {
		return pid.nodeID + "/" + pid.id
	}
	return pid.id
}

// IsRemote 判断是否为远程 actor
func (pid *PID) IsRemote() bool {
	return pid != nil && pid.nodeID != ""
}

// NodeID 返回节点ID，空字符串表示本地
func (pid *PID) NodeID() string {
	if pid == nil {
		return ""
	}
	return pid.nodeID
}

// ActorID 返回 actor ID
func (pid *PID) ActorID() string {
	if pid == nil {
		return ""
	}
	return pid.id
}

// Tell sends a message to this actor (fire-and-forget).
// 自动判断本地/远程并路由。
func (pid *PID) Tell(message interface{}) {
	if pid == nil || pid.system == nil {
		return
	}
	pid.system.send(pid, message, nil)
}

// NewRemotePID 创建远程 actor 的 PID
// nodeID: 远程节点ID
// actorID: 远程 actor ID
// system: 本地 ActorSystem（用于发送消息）
func NewRemotePID(nodeID, actorID string, system *ActorSystem) *PID {
	if nodeID == "" || actorID == "" || system == nil {
		return nil
	}
	return &PID{
		id:     actorID,
		nodeID: nodeID,
		system: system,
	}
}
