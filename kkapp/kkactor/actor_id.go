package kkactor

import (
	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/kkactor/actorremotes"
	"github.com/vvisun/kkdg/kkerrors"
)

// LucencyID 是 Actor 的唯一标识。
// 不需要关心 Actor 所在节点，由 ActorLocator 自动判断本地/远程：
// 如果【NodeID 为空字符串】或【NodeID 在当前进程的任意节点中存在】，则认为是本地 Actor；否则为远程 Actor。
//
//	unique id = nodeID + actorKey。
//
// 不采用字符串拼接而使用结构体表示，
// 一是字符串拼接拆解消耗，二是必须强行规定字符串格式，容易出错。
// 比如nodeID和actorKey如果用"-"连接，需要强行规定nodeID和actorKey里不能含"-"，否则就会解析出错。
type LucencyID struct {
	// 节点ID。为空表示本地Actor。
	nodeID string
	// actor 标识，节点内唯一。
	actorKey string
}

// NodeID 返回逻辑节点 ID。
func (id LucencyID) NodeID() string {
	return id.nodeID
}

// ActorKey 返回逻辑 actor 标识。
func (id LucencyID) ActorKey() string {
	return id.actorKey
}

// 创建LucencyID
func NewLucencyID(nodeId, actorKey string) (LucencyID, error) {
	if !kkapp.IsValidActorKey(actorKey) {
		return LucencyID{}, kkerrors.ErrActorInvalidActorKey
	}
	if !kkapp.IsValidActorNodeId(nodeId) {
		return LucencyID{}, kkerrors.ErrActorInvalidNodeId
	}
	id := LucencyID{
		nodeID:   nodeId,
		actorKey: actorKey,
	}
	return id, nil
}

// ActorRef2LucencyID 将 actorremotes.ActorRef 转换为 LucencyID。
func ActorRef2LucencyID(actorRef *actorremotes.ActorRef) (LucencyID, error) {
	if actorRef == nil {
		return LucencyID{}, kkerrors.ErrActorInvalidActorRef
	}
	if !kkapp.IsValidActorKey(actorRef.ActorKey) {
		return LucencyID{}, kkerrors.ErrActorInvalidActorKey
	}
	if !kkapp.IsValidActorNodeId(actorRef.NodeID) {
		return LucencyID{}, kkerrors.ErrActorInvalidNodeId
	}
	return NewLucencyID(actorRef.NodeID, actorRef.ActorKey)
}

// LucencyID2ActorRef 将 LucencyID 转换为 actorremotes.ActorRef。
func LucencyID2ActorRef(lucencyID LucencyID) actorremotes.ActorRef {
	return actorremotes.ActorRef{
		NodeID:   lucencyID.NodeID(),
		ActorKey: lucencyID.ActorKey(),
	}
}
