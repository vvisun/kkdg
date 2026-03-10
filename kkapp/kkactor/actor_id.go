package kkactor

import (
	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkerrors"
)

// LucencyActorID 是 IActorID 的默认实现。
// 不需要关心 Actor 所在节点，由 ActorLocator 自动判断本地/远程：
// 如果【NodeID 为空字符串】或【NodeID 在当前进程的任意节点中存在】，则认为是本地 Actor；否则为远程 Actor。
type LucencyActorID struct {
	nodeID   string // 节点ID，【英文字母、数字】组合，如 "game1"、"game2"。为空表示本地Actor。
	actorKey string // actor 标识， 【英文字母、数字、下划线("_") 组合】，如 "ccgame_main"、"gate_router"
}

// NodeID 返回逻辑节点 ID。
func (id LucencyActorID) NodeID() string {
	return id.nodeID
}

// ActorKey 返回逻辑 actor 标识。
func (id LucencyActorID) ActorKey() string {
	return id.actorKey
}

// 创建LucencyActorID
func NewLucencyActorID(nodeId, actorKey string) (LucencyActorID, error) {
	if !kkapp.IsValidActorKey(actorKey) {
		return LucencyActorID{}, kkerrors.ErrActorInvalidActorKey
	}
	if !kkapp.IsValidActorNodeId(nodeId) {
		return LucencyActorID{}, kkerrors.ErrActorInvalidNodeId
	}
	id := LucencyActorID{
		nodeID:   nodeId,
		actorKey: actorKey,
	}
	return id, nil
}
