package kkactor

import (
	"strings"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kklog"
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

// 拼接actorKey，一般用于父子actor之间拼接。
// 例如："game_player"、"gate_router"
func CombineActorKeys(actorKeys ...string) (string, error) {
	if len(actorKeys) == 0 {
		return "", kkerrors.ErrActorInvalidActorKeys
	}
	return strings.Join(actorKeys, kkapp.ActorKeySep_ParantAndChild), nil
}

// 拼接完整的actor寻址名称。
// 例如："game1/game_player"、"gate1/gate_router"
func CombineNodeAndActorKey(nodeId string, actorKeys ...string) (string, error) {
	actorKey, err := CombineActorKeys(actorKeys...)
	if err != nil {
		return "", err
	}
	return nodeId + kkapp.ActorKeySep_NodeAndActor + actorKey, nil
}

// id to name
// LucencyActorID只能通过本包的NewLucencyActorID创建，故这里必定已经是合法的。
func GetActorName(actorId LucencyActorID) (string, error) {
	if !kkapp.IsValidActorNodeId(actorId.nodeID) {
		return "", kkerrors.ErrActorInvalidNodeId
	}
	if !kkapp.IsValidActorKey(actorId.actorKey) {
		return "", kkerrors.ErrActorInvalidActorKey
	}
	return actorId.nodeID + kkapp.ActorKeySep_NodeAndActor + actorId.actorKey, nil
}

// name to id
func GetActorId(actorName string) (LucencyActorID, error) {
	// 第1个分隔符之前的是NodeID，之后的是ActorKey。
	NodeID, ActorKey, found := strings.Cut(actorName, kkapp.ActorKeySep_NodeAndActor)
	if !found {
		if !kkapp.IsValidActorKey(actorName) {
			kklog.Errorf("invalid actor name: %s", actorName)
			return LucencyActorID{}, kkerrors.ErrActorInvalidActorKey
		}
		return LucencyActorID{
			nodeID:   "",
			actorKey: actorName,
		}, nil
	}
	if !kkapp.IsValidActorNodeId(NodeID) {
		kklog.Errorf("invalid actor name: %s, node id: %s", actorName, NodeID)
		return LucencyActorID{}, kkerrors.ErrActorInvalidNodeId
	}
	if !kkapp.IsValidActorKey(ActorKey) {
		kklog.Errorf("invalid actor name: %s, actor key: %s", actorName, ActorKey)
		return LucencyActorID{}, kkerrors.ErrActorInvalidActorKey
	}
	return LucencyActorID{
		nodeID:   NodeID,
		actorKey: ActorKey,
	}, nil
}
