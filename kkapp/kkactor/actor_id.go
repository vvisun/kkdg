package kkactor

import "strings"

const ActorKeySeparator = "/"

// LucencyActorID 是 IActorID 的默认实现。
// 不需要关心 Actor 所在节点，由 ActorLocator 自动判断本地/远程：
// 如果【NodeID 为空字符串】或【NodeID 在当前进程的任意节点中存在】，则认为是本地 Actor；否则为远程 Actor。
type LucencyActorID struct {
	nodeID    string // 节点ID，如 game1、game2。为空表示本地Actor。
	actorKey  string // actor 标识，如 "ccgame/main"、"gate/router"
	actorName string // actor 名称。//缓存以优化性能
}

// NodeID 返回逻辑节点 ID。
func (id LucencyActorID) NodeID() string {
	return id.nodeID
}

// ActorKey 返回逻辑 actor 标识。
func (id LucencyActorID) ActorKey() string {
	return id.actorKey
}

// ActorName 返回 actor 名称。
func (id LucencyActorID) ActorName() string {
	return id.actorName
}

func NewLucencyActorID(nodeId, actorKey string) LucencyActorID {
	id := LucencyActorID{
		nodeID:   nodeId,
		actorKey: actorKey,
	}
	id.actorName = GetActorName(id) //缓存以优化性能
	return id
}

// id to name
func GetActorName(actorId LucencyActorID) string {
	return actorId.nodeID + ActorKeySeparator + actorId.actorKey
}

// name to id
func GetActorId(actorName string) LucencyActorID {
	// 第1个分隔符之前的是NodeID，之后的是ActorKey。
	NodeID, ActorKey, found := strings.Cut(actorName, ActorKeySeparator)
	if !found {
		return LucencyActorID{
			nodeID:   "",
			actorKey: actorName,
		}
	}
	return LucencyActorID{
		nodeID:   NodeID,
		actorKey: ActorKey,
	}
}
