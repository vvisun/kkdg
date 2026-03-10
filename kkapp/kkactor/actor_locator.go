package kkactor

import (
	"strings"
	"sync"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp"
)

const ActorKeySeparator = "/"

// RemoteActorID 透明化Actor寻址，不需要关心Actor所在节点，ActorLocator自动判断是本地还是远程Actor。
// 如果【NodeID为空字符串】或【NodeID在当前进程的任意节点中存在】，则认为是本地Actor。否则认为是远程Actor。
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

var (
	globalActorFramework *ActorFramework
	onceActorFramework   sync.Once
)

// 获取全局Actor框架, 线上一般用全局即可，避免混乱。
func GetGlobalActorFramework() *ActorFramework {
	onceActorFramework.Do(func() {
		globalActorFramework = NewActorFramework(NewActorLocator(), actor.NewActorSystem())
	})
	return globalActorFramework
}

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
		nodes[node.GetNodeId()] = node
	}
	return &ActorLocator{
		actors: make(map[string]*actor.PID),
		nodes:  nodes,
	}
}

func (slf *ActorLocator) AddNode(node *kkapp.NodeInfo) {
	if node == nil {
		return
	}
	slf.mu.Lock()
	slf.nodes[node.GetNodeId()] = node
	slf.mu.Unlock()
}

func (slf *ActorLocator) RemoveNode(node *kkapp.NodeInfo) {
	if node == nil {
		return
	}
	slf.mu.Lock()
	delete(slf.nodes, node.GetNodeId())
	slf.mu.Unlock()
}

func (slf *ActorLocator) GetActor(id RemoteActorID) *actor.PID {
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

// 判断Actor是否是本地Actor。
func (slf *ActorLocator) IsLocalActor(id RemoteActorID) bool {
	if id.NodeID == "" {
		return true
	}
	_, ok := slf.nodes[id.NodeID]
	if !ok {
		return false
	}
	return true
}

// 判断Actor是否是远程Actor。
func (slf *ActorLocator) IsRemoteActor(id RemoteActorID) bool {
	return !slf.IsLocalActor(id)
}

// 判断ActorName是否是本地Actor。
func (slf *ActorLocator) IsLocalActorName(actorName string) bool {
	id := GetActorId(actorName)
	return slf.IsLocalActor(id)
}

// 判断ActorName是否是远程Actor。
func (slf *ActorLocator) IsRemoteActorName(actorName string) bool {
	id := GetActorId(actorName)
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
