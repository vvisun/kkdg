package kkactor

import (
	"strings"

	"github.com/vvisun/kkdg/kkapp"
)

// LucencyActorID 是 IActorID 的默认实现。
// 不需要关心 Actor 所在节点，由 ActorLocator 自动判断本地/远程：
// 如果【NodeID 为空字符串】或【NodeID 在当前进程的任意节点中存在】，则认为是本地 Actor；否则为远程 Actor。
type LucencyActorID struct {
	nodeID   string // 节点ID，如 game1、game2。为空表示本地Actor。
	actorKey string // actor 标识，如 "ccgame/main"、"gate/router"
}

// NodeID 返回逻辑节点 ID。
func (id LucencyActorID) NodeID() string {
	return id.nodeID
}

// ActorKey 返回逻辑 actor 标识。
func (id LucencyActorID) ActorKey() string {
	return id.actorKey
}

func NewLucencyActorID(nodeId, actorKey string) LucencyActorID {
	if !IsValidActorKey(actorKey) {
		panic("invalid actor key: " + actorKey)
	}
	if !IsValidNodeId(nodeId) {
		panic("invalid node id: " + nodeId)
	}
	id := LucencyActorID{
		nodeID:   nodeId,
		actorKey: actorKey,
	}
	return id
}

func IsValidNodeId(nodeId string) bool {
	if nodeId == "" {
		return true //允许空字符串，表示本地Actor
	}
	if strings.Contains(nodeId, kkapp.ActorKeySeparator) {
		return false //不允许包含分隔符
	}
	return true
}

// 英文字母、数字、下划线("_") 组合，例如 "ccgame_main"、"gate_router"
func IsValidActorKey(actorKey string) bool {
	if actorKey == "" {
		return false
	}
	if strings.Contains(actorKey, kkapp.ActorKeySeparator) {
		return false
	}
	return true
}

// id to name
func GetActorName(actorId LucencyActorID) string {
	return actorId.nodeID + kkapp.ActorKeySeparator + actorId.actorKey
}

// name to id
func GetActorId(actorName string) LucencyActorID {
	// 第1个分隔符之前的是NodeID，之后的是ActorKey。
	NodeID, ActorKey, found := strings.Cut(actorName, kkapp.ActorKeySeparator)
	if !found {
		if !IsValidActorKey(actorName) {
			panic("invalid actor key: " + actorName)
		}
		return LucencyActorID{
			nodeID:   "",
			actorKey: actorName,
		}
	}
	if !IsValidActorKey(ActorKey) {
		panic("invalid actor key: " + ActorKey)
	}
	return LucencyActorID{
		nodeID:   NodeID,
		actorKey: ActorKey,
	}
}
