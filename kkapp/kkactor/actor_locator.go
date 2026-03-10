package kkactor

import (
	"sync"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkerrors"
)

// Actor寻址系统
type ActorLocator struct {
	mu     sync.RWMutex
	actors map[string]*actor.PID      // actorName -> *actor.PID 当前进程的所有Actor信息
	nodes  map[string]*kkapp.NodeInfo // nodeId -> kkapp.IApplication 当前进程的所有节点信息
}

// 创建Actor寻址系统，localNodes为当前进程的本地节点信息。
// 这里之所以允许传入多个localNode，是因为单机部署时，可以直接在同一个进程里启动多个节点。
// 在单机部署的情况下，直接本地寻址，性能更好。
func NewActorLocator(localNodes ...*kkapp.NodeInfo) *ActorLocator {
	nodes := make(map[string]*kkapp.NodeInfo)
	for _, node := range localNodes {
		if !kkapp.IsValidActorNodeId(node.GetNodeId()) {
			panic("invalid node id") //一般都是启动时配置节点信息。非法id直接panic，避免影响后续逻辑
		}
		nodes[node.GetNodeId()] = node
	}
	return &ActorLocator{
		actors: make(map[string]*actor.PID),
		nodes:  nodes,
	}
}

func (slf *ActorLocator) AddNode(node *kkapp.NodeInfo) error {
	if node == nil {
		return kkerrors.ErrActorAddInvalidNode
	}
	if !kkapp.IsValidActorNodeId(node.GetNodeId()) {
		return kkerrors.ErrActorInvalidNodeId
	}
	slf.mu.Lock()
	slf.nodes[node.GetNodeId()] = node
	slf.mu.Unlock()
	return nil
}

func (slf *ActorLocator) RemoveNode(node *kkapp.NodeInfo) error {
	if node == nil {
		return kkerrors.ErrActorAddInvalidNode
	}
	if !kkapp.IsValidActorNodeId(node.GetNodeId()) {
		return kkerrors.ErrActorInvalidNodeId
	}
	nodeId := node.GetNodeId()
	slf.mu.Lock()
	// 如果移除的是本地节点，则需要移除本地Actor。
	for k := range slf.actors {
		id, err := GetActorId(k)
		if err != nil {
			continue
		}
		ok, err := slf.isLocalActor(id)
		if err != nil {
			continue
		}
		if id.nodeID == nodeId && ok {
			delete(slf.actors, k)
		}
	}
	delete(slf.nodes, nodeId)
	slf.mu.Unlock()
	return nil
}

func (slf *ActorLocator) GetActor(id LucencyActorID) (*actor.PID, error) {
	actorName, err := GetActorName(id)
	if err != nil {
		return nil, err
	}
	slf.mu.RLock()
	pid, ok := slf.actors[actorName]
	slf.mu.RUnlock()
	if !ok {
		return nil, kkerrors.ErrActorNotFound
	}
	return pid, nil
}

func (slf *ActorLocator) AddActor(id LucencyActorID, pid *actor.PID) error {
	if !kkapp.IsValidActorKey(id.actorKey) {
		return kkerrors.ErrActorInvalidActorKey
	}
	if !kkapp.IsValidActorNodeId(id.nodeID) {
		return kkerrors.ErrActorInvalidNodeId
	}
	if pid == nil {
		return kkerrors.ErrActorAddInvalidPID
	}
	actorName, err := GetActorName(id)
	if err != nil {
		return err
	}
	slf.mu.Lock()
	slf.actors[actorName] = pid
	slf.mu.Unlock()
	return nil
}

func (slf *ActorLocator) RemoveActor(id LucencyActorID) error {
	actorName, err := GetActorName(id)
	if err != nil {
		return err
	}
	slf.mu.Lock()
	delete(slf.actors, actorName)
	slf.mu.Unlock()
	return nil
}

func (slf *ActorLocator) isLocalActor(id LucencyActorID) (bool, error) {
	if !kkapp.IsValidActorNodeId(id.nodeID) {
		return false, kkerrors.ErrActorInvalidNodeId
	}
	if !kkapp.IsValidActorKey(id.actorKey) {
		return false, kkerrors.ErrActorInvalidActorKey
	}
	if id.nodeID == "" {
		return true, nil
	}
	_, ok := slf.nodes[id.nodeID]
	return ok, nil
}

// 判断Actor是否是本地Actor。
func (slf *ActorLocator) IsLocalActor(id LucencyActorID) (bool, error) {
	slf.mu.RLock()
	ok, err := slf.isLocalActor(id)
	slf.mu.RUnlock()
	return ok, err
}

// 判断Actor是否是远程Actor。
func (slf *ActorLocator) IsRemoteActor(id LucencyActorID) (bool, error) {
	ok, err := slf.IsLocalActor(id)
	return !ok, err
}

// 判断ActorName是否是本地Actor。
func (slf *ActorLocator) IsLocalActorName(actorName string) (bool, error) {
	id, err := GetActorId(actorName)
	if err != nil {
		return false, err
	}
	return slf.IsLocalActor(id)
}

// 判断ActorName是否是远程Actor。
func (slf *ActorLocator) IsRemoteActorName(actorName string) (bool, error) {
	id, err := GetActorId(actorName)
	if err != nil {
		return false, err
	}
	return slf.IsRemoteActor(id)
}

// 遍历nodes, fn返回false时停止遍历
func (slf *ActorLocator) ForEachNode(fn func(node *kkapp.NodeInfo) bool) {
	slf.mu.RLock()
	defer slf.mu.RUnlock()
	for _, node := range slf.nodes {
		if !fn(node) {
			break
		}
	}
}

// 遍历actors, fn返回false时停止遍历
func (slf *ActorLocator) ForEachActor(fn func(actorName string, pid *actor.PID) bool) {
	slf.mu.RLock()
	defer slf.mu.RUnlock()
	for actorName, pid := range slf.actors {
		if !fn(actorName, pid) {
			break
		}
	}
}
