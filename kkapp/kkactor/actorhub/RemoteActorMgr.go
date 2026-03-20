package actorhub

import (
	"sync"

	"github.com/vvisun/kkdg/kkapp/kkactor"
	"github.com/vvisun/kkdg/kkerrors"
)

// IServerRemoteActorMgr 服务器端远程Actor管理器接口。
type IServerRemoteActorMgr interface {
	// 注册actor
	RegisterActor(actorID kkactor.LucencyActorID, rpcAddr string) error
	// 注销actor
	UnregisterActor(actorID kkactor.LucencyActorID) error
}

// IClientRemoteActorMgr 客户端远程Actor管理器接口。
type IClientRemoteActorMgr interface {
	// 寻找actor
	FindActor(actorID kkactor.LucencyActorID) (*RemoteActor, error)
	// 获取某个节点上的所有actor列表
	GetAllActorsOfNode(nodeID string) []*RemoteActor
}

// RemoteActor 远程Actor信息。
type RemoteActor struct {
	actorID kkactor.LucencyActorID // ActorID
	rpcAddr string                 // 远程RPC地址
}

// RemoteActorMgr 远程Actor管理器。
//
//	hubserver 与 hubclient 可以通过这个管理器缓存远程Actor信息。
type RemoteActorMgr struct {
	actors map[kkactor.LucencyActorID]*RemoteActor
	mu     sync.RWMutex
}

var _ IServerRemoteActorMgr = &RemoteActorMgr{}
var _ IClientRemoteActorMgr = &RemoteActorMgr{}

func (slf *RemoteActorMgr) RegisterActor(actorID kkactor.LucencyActorID, rpcAddr string) error {
	lucId, err := kkactor.NewLucencyActorID(actorID.NodeID(), actorID.ActorKey())
	if err != nil {
		return err
	}
	slf.mu.Lock()
	defer slf.mu.Unlock()
	slf.actors[lucId] = &RemoteActor{
		actorID: lucId,
		rpcAddr: rpcAddr,
	}
	return nil
}

func (slf *RemoteActorMgr) UnregisterActor(actorID kkactor.LucencyActorID) error {
	lucId, err := kkactor.NewLucencyActorID(actorID.NodeID(), actorID.ActorKey())
	if err != nil {
		return err
	}
	slf.mu.Lock()
	defer slf.mu.Unlock()
	delete(slf.actors, lucId)
	return nil
}

func (slf *RemoteActorMgr) FindActor(lucId kkactor.LucencyActorID) (*RemoteActor, error) {
	slf.mu.RLock()
	defer slf.mu.RUnlock()
	actor, ok := slf.actors[lucId]
	if !ok {
		return nil, kkerrors.ErrActorNotFound
	}
	return actor, nil
}

func (slf *RemoteActorMgr) GetAllActorsOfNode(nodeID string) []*RemoteActor {
	slf.mu.RLock()
	defer slf.mu.RUnlock()
	actors := make([]*RemoteActor, 0)
	for _, actor := range slf.actors {
		if actor.actorID.NodeID() == nodeID {
			actors = append(actors, actor)
		}
	}
	return actors
}
