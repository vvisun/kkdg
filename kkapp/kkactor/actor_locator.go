package kkactor

import (
	"sync"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kklog"
)

// Actor寻址系统
type ActorLocator struct {
	mu     sync.RWMutex
	actors map[LucencyID]*actor.PID   // LucencyActorID -> *actor.PID 当前进程的所有Actor信息
	nodes  map[string]*kkapp.NodeInfo // nodeId -> kkapp.IApplication 当前进程的所有节点信息
}

// 创建Actor寻址系统，localNodes为当前进程的本地节点信息。
// 这里之所以允许传入多个localNode，是因为单机部署时，可以直接在同一个进程里启动多个节点。
// 在单机部署的情况下，直接本地寻址，性能更好。
func NewActorLocator(localNodes ...*kkapp.NodeInfo) *ActorLocator {
	nodes := make(map[string]*kkapp.NodeInfo)
	for _, node := range localNodes {
		if node == nil {
			kklog.Warnf("new actor locator with nil node: %v", node)
			continue
		}
		if !kkapp.IsValidActorNodeId(node.GetNodeId()) {
			// 启动期间的异常装配直接panic，不然反而将隐含问题带到了运行期间，造成不可预测的错误
			kklog.PanicLog("invalid node id")
		}
		nodes[node.GetNodeId()] = node
	}
	return &ActorLocator{
		actors: make(map[LucencyID]*actor.PID),
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
	nodeId := node.GetNodeId()
	slf.mu.Lock()
	// 如果移除的是本地节点，则需要移除本地Actor。
	if slf.isLocalNode(nodeId) {
		for id := range slf.actors {
			ok, err := slf.isLocalActor(id)
			if err != nil {
				continue
			}
			if id.nodeID == nodeId && ok {
				delete(slf.actors, id)
			}
		}
	}
	delete(slf.nodes, nodeId)
	slf.mu.Unlock()
	return nil
}

func (slf *ActorLocator) isLocalNode(nodeId string) bool {
	if nodeId == "" {
		return true //空nodeId表示本地Node
	}
	_, ok := slf.nodes[nodeId]
	return ok
}

func (slf *ActorLocator) GetLocalActor(nodeID, actorKey string) (*actor.PID, error) {
	return slf.GetActor(LucencyID{nodeID: nodeID, actorKey: actorKey})
}

func (slf *ActorLocator) GetActor(id LucencyID) (*actor.PID, error) {
	slf.mu.RLock()
	pid, ok := slf.actors[id]
	slf.mu.RUnlock()
	if !ok {
		return nil, kkerrors.ErrActorNotFound
	}
	return pid, nil
}

func (slf *ActorLocator) AddActor(id LucencyID, pid *actor.PID) error {
	if !kkapp.IsValidActorKey(id.actorKey) {
		return kkerrors.ErrActorInvalidActorKey
	}
	if !kkapp.IsValidActorNodeId(id.nodeID) {
		return kkerrors.ErrActorInvalidNodeId
	}
	if pid == nil {
		return kkerrors.ErrActorAddInvalidPID
	}
	slf.mu.Lock()
	slf.actors[id] = pid
	slf.mu.Unlock()
	return nil
}

func (slf *ActorLocator) AddActorEx(nodeId, actorKey string, pid *actor.PID) error {
	if !kkapp.IsValidActorKey(actorKey) {
		return kkerrors.ErrActorInvalidActorKey
	}
	if !kkapp.IsValidActorNodeId(nodeId) {
		return kkerrors.ErrActorInvalidNodeId
	}
	if pid == nil {
		return kkerrors.ErrActorAddInvalidPID
	}
	id := LucencyID{
		nodeID:   nodeId,
		actorKey: actorKey,
	}
	slf.mu.Lock()
	slf.actors[id] = pid
	slf.mu.Unlock()
	return nil
}

func (slf *ActorLocator) RemoveActor(id LucencyID) error {
	slf.mu.Lock()
	delete(slf.actors, id)
	slf.mu.Unlock()
	return nil
}

func (slf *ActorLocator) isLocalActor(id LucencyID) (bool, error) {
	if !kkapp.IsValidActorNodeId(id.nodeID) {
		return false, kkerrors.ErrActorInvalidNodeId
	}
	if !kkapp.IsValidActorKey(id.actorKey) {
		return false, kkerrors.ErrActorInvalidActorKey
	}
	_, ok := slf.nodes[id.nodeID]
	return ok, nil //如果nodeID在nodes中，则认为是本地Actor
}

// 判断Actor是否是本地Actor。
func (slf *ActorLocator) IsLocalActor(id LucencyID) (bool, error) {
	slf.mu.RLock()
	ok, err := slf.isLocalActor(id)
	slf.mu.RUnlock()
	return ok, err
}

// 判断Actor是否是远程Actor。
func (slf *ActorLocator) IsRemoteActor(id LucencyID) (bool, error) {
	ok, err := slf.IsLocalActor(id)
	return !ok, err
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
func (slf *ActorLocator) ForEachActor(fn func(id LucencyID, pid *actor.PID) bool) {
	slf.mu.RLock()
	defer slf.mu.RUnlock()
	for id, pid := range slf.actors {
		if !fn(id, pid) {
			break
		}
	}
}
