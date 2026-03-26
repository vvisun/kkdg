package kkactor

import (
	"sync"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp/achecker"
	"github.com/vvisun/kkdg/kkapp/kkactor/transport/actortrans"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kklog"
)

// LocalActorManager 本进程内本地 Actor 管理：已注册 PID（actors）与本进程承载的本地 nodeID 集合（localNodes）。
//
// 本地 / 远程（供 ActorFramework 路由）：LucencyID 的 nodeID 已在 localNodes 中即为本地。
// AddActor 时若该 nodeID 尚未在 localNodes 中，会自动加入（仅占位，无 NodeType 等元数据）。
//
// 完整 *kkapp.NodeInfo（类型、地址、RPC 等）不由 Locator 持有；需要时可由应用侧单独的本地 NodeInfo 管理器维护。
//
// 远程 Actor 目录由 registry/actorhub.RemoteActorMgr 等在传输或注册侧维护，不挂在 Locator 上。
type LocalActorManager struct {
	mu         sync.RWMutex
	actors     map[LucencyID]*actor.PID
	localNodes map[string]struct{} // 本进程视为本地的 nodeID 集合
}

// NewLocalActorManager 可选传入初始本地 nodeID（须通过 IsValidActorNodeId）。
func NewLocalActorManager(localNodeIDs ...string) *LocalActorManager {
	localNodes := make(map[string]struct{})
	for _, id := range localNodeIDs {
		if !achecker.IsValidActorNodeId(id) {
			kklog.PanicLog("invalid node id")
		}
		localNodes[id] = struct{}{}
	}
	return &LocalActorManager{
		actors:     make(map[LucencyID]*actor.PID),
		localNodes: localNodes,
	}
}

// AddLocalNode 将 nodeID 记入本进程本地节点集合（仅路由语义，不含 NodeInfo）。
func (slf *LocalActorManager) AddLocalNode(nodeID string) error {
	if !achecker.IsValidActorNodeId(nodeID) {
		return kkerrors.ErrActorInvalidNodeId
	}
	slf.mu.Lock()
	slf.localNodes[nodeID] = struct{}{}
	slf.mu.Unlock()
	return nil
}

// RemoveLocalNode 从本地节点集合移除 nodeID，并删除该 nodeID 下已登记的所有 Actor。
func (slf *LocalActorManager) RemoveLocalNode(nodeID string) error {
	if !achecker.IsValidActorNodeId(nodeID) {
		return kkerrors.ErrActorInvalidNodeId
	}
	slf.mu.Lock()
	for id := range slf.actors {
		if id.nodeID == nodeID {
			delete(slf.actors, id)
		}
	}
	delete(slf.localNodes, nodeID)
	slf.mu.Unlock()
	return nil
}

// IsLocalActor 当且仅当该 LucencyID 的 nodeID 已在本 Locator 的 localNodes 中。
func (slf *LocalActorManager) IsLocalActor(id LucencyID) (bool, error) {
	if !achecker.IsValidActorNodeId(id.nodeID) {
		return false, kkerrors.ErrActorInvalidNodeId
	}
	if !achecker.IsValidActorKey(id.actorKey) {
		return false, kkerrors.ErrActorInvalidActorKey
	}
	slf.mu.RLock()
	_, ok := slf.localNodes[id.nodeID]
	slf.mu.RUnlock()
	return ok, nil
}

// IsRemoteActor 在 LucencyID 合法的前提下，等价于 !IsLocalActor。
func (slf *LocalActorManager) IsRemoteActor(id LucencyID) (bool, error) {
	ok, err := slf.IsLocalActor(id)
	if err != nil {
		return false, err
	}
	return !ok, nil
}

// GetLocalActor 查找本进程已登记的 PID。
func (slf *LocalActorManager) GetLocalActor(actorRef *actortrans.ActorRef) (*actor.PID, error) {
	if actorRef == nil {
		return nil, kkerrors.ErrActorInvalidActorRef
	}
	return slf.GetActor(LucencyID{nodeID: actorRef.NodeID, actorKey: actorRef.ActorKey})
}

// GetActor 根据 LucencyID 查找本进程已登记的 PID。
func (slf *LocalActorManager) GetActor(id LucencyID) (*actor.PID, error) {
	slf.mu.RLock()
	pid, ok := slf.actors[id]
	slf.mu.RUnlock()
	if !ok {
		return nil, kkerrors.ErrActorNotFound
	}
	return pid, nil
}

// AddActor 登记本地 Actor；若 nodeID 尚未在 localNodes 中，自动加入该 nodeID。
func (slf *LocalActorManager) AddActor(id LucencyID, pid *actor.PID) error {
	if !achecker.IsValidActorKey(id.actorKey) {
		return kkerrors.ErrActorInvalidActorKey
	}
	if !achecker.IsValidActorNodeId(id.nodeID) {
		return kkerrors.ErrActorInvalidNodeId
	}
	if pid == nil {
		return kkerrors.ErrActorAddInvalidPID
	}
	slf.mu.Lock()
	slf.localNodes[id.nodeID] = struct{}{}
	slf.actors[id] = pid
	slf.mu.Unlock()
	return nil
}

// RemoveActor 从本 Locator 移除指定 LucencyID 的 PID（不删除 localNodes 中的 nodeID）。
func (slf *LocalActorManager) RemoveActor(id LucencyID) error {
	slf.mu.Lock()
	delete(slf.actors, id)
	slf.mu.Unlock()
	return nil
}

// ForEachLocalNodeID 遍历已登记的本地 nodeID；fn 返回 false 时停止。遍历顺序未定义。
func (slf *LocalActorManager) ForEachLocalNodeID(fn func(nodeID string) bool) {
	slf.mu.RLock()
	defer slf.mu.RUnlock()
	for id := range slf.localNodes {
		if !fn(id) {
			break
		}
	}
}

// ForEachActor 遍历已登记 Actor；fn 返回 false 时停止。
func (slf *LocalActorManager) ForEachActor(fn func(id LucencyID, pid *actor.PID) bool) {
	slf.mu.RLock()
	defer slf.mu.RUnlock()
	for id, pid := range slf.actors {
		if !fn(id, pid) {
			break
		}
	}
}
