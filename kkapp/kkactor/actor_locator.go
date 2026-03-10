package kkactor

import (
	"strings"
	"sync"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp"
)

const ActorKeySeparator = "/"

// RemoteActorID 透明化Actor寻址，不需要关心Actor所在节点，只需要关心ActorKey。
type RemoteActorID struct {
	NodeID   string // 逻辑节点ID，如 game1、game2。为空表示本地Actor。
	ActorKey string // 逻辑 actor 标识，如 "ccgame/main"、"gate/router"
}

func GetActorName(actorId RemoteActorID) string {
	return actorId.NodeID + ActorKeySeparator + actorId.ActorKey
}

func GetActorId(actorName string) RemoteActorID {
	// 第1个分隔符之前的是NodeID，之后的是ActorKey。
	NodeID, ActorKey, found := strings.Cut(actorName, ActorKeySeparator)
	if !found {
		return RemoteActorID{
			NodeID:   "",
			ActorKey: actorName,
		}
	}
	return RemoteActorID{
		NodeID:   NodeID,
		ActorKey: ActorKey,
	}
}

//----------------------------------------------------------

// Actor寻址系统
type ActorLocator struct {
	mu     sync.RWMutex
	actors map[string]*actor.PID
	node   *kkapp.NodeInfo
}

// 创建Actor寻址系统，localNode为当前节点信息。
func NewActorLocator(localNode *kkapp.NodeInfo) *ActorLocator {
	if localNode == nil {
		panic("localNode is nil")
	}
	return &ActorLocator{
		actors: make(map[string]*actor.PID),
		node:   localNode,
	}
}

func (slf *ActorLocator) LocateActor(id RemoteActorID) *actor.PID {
	actorName := GetActorName(id)
	slf.mu.RLock()
	pid, ok := slf.actors[actorName]
	slf.mu.RUnlock()
	if !ok {
		return nil
	}
	return pid
}

func (slf *ActorLocator) AddActor(id RemoteActorID, pid *actor.PID) {
	actorName := GetActorName(id)
	slf.mu.Lock()
	slf.actors[actorName] = pid
	slf.mu.Unlock()
}

func (slf *ActorLocator) RemoveActor(id RemoteActorID) {
	actorName := GetActorName(id)
	slf.mu.Lock()
	delete(slf.actors, actorName)
	slf.mu.Unlock()
}

func (slf *ActorLocator) IsLocalActor(id RemoteActorID) bool {
	return id.NodeID == slf.node.GetNodeId() || id.NodeID == ""
}

func (slf *ActorLocator) IsRemoteActor(id RemoteActorID) bool {
	return !slf.IsLocalActor(id)
}

func (slf *ActorLocator) IsLocalActorName(actorName string) bool {
	id := GetActorId(actorName)
	return slf.IsLocalActor(id)
}

func (slf *ActorLocator) IsRemoteActorName(actorName string) bool {
	id := GetActorId(actorName)
	return slf.IsRemoteActor(id)
}
