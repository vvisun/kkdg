package actorhub

import (
	"sync"

	"github.com/vvisun/kkdg/kkapp/kkactor"
	"github.com/vvisun/kkdg/kkerrors"
)

// IServerRemoteActorMgr 服务器端远程Actor管理器接口。
type IServerRemoteActorMgr interface {
	// 注册actor
	RegisterActor(actorID kkactor.LucencyID, rpcAddr string) error
	// 注销actor
	UnregisterActor(actorID kkactor.LucencyID) error
	// 寻找actor
	FindActor(actorID kkactor.LucencyID) (*RemoteActor, error)
	// 获取某个节点上的所有actor列表
	GetAllActorsOfNode(nodeID string) []*RemoteActor
}

// IClientRemoteActorMgr 客户端远程Actor管理器接口。
type IClientRemoteActorMgr interface {
	// 寻找actor
	FindActor(actorID kkactor.LucencyID) (*RemoteActor, error)
	// 获取某个节点上的所有actor列表
	GetAllActorsOfNode(nodeID string) []*RemoteActor
}

// RemoteActor 远程Actor信息。
type RemoteActor struct {
	actorID kkactor.LucencyID // ActorID
	rpcAddr string            // 远程RPC地址
}

// LucencyID 返回透明 Actor ID（供 Hub 等组包使用）。
func (r *RemoteActor) LucencyID() kkactor.LucencyID {
	return r.actorID
}

// RpcAddress 返回该 Actor 所在节点的 RPC 监听地址（Hub 注册表中的值）。
func (r *RemoteActor) RpcAddress() string {
	return r.rpcAddr
}

// RemoteActorMgr 远程 Actor 目录缓存（LucencyID → 节点 RPC 等元数据）。
// 由 Hub 服务端、Hub 客户端或依赖目录的传输实现持有；不参与 ActorFramework 的本地 PID 路由。
type RemoteActorMgr struct {
	actors map[kkactor.LucencyID]*RemoteActor
	mu     sync.RWMutex
}

func NewRemoteActorMgr() *RemoteActorMgr {
	return &RemoteActorMgr{
		actors: make(map[kkactor.LucencyID]*RemoteActor),
	}
}

var _ IServerRemoteActorMgr = &RemoteActorMgr{}
var _ IClientRemoteActorMgr = &RemoteActorMgr{}

func (slf *RemoteActorMgr) RegisterActor(actorID kkactor.LucencyID, rpcAddr string) error {
	lucId, err := kkactor.NewLucencyID(actorID.NodeID(), actorID.ActorKey())
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

func (slf *RemoteActorMgr) UnregisterActor(actorID kkactor.LucencyID) error {
	lucId, err := kkactor.NewLucencyID(actorID.NodeID(), actorID.ActorKey())
	if err != nil {
		return err
	}
	slf.mu.Lock()
	defer slf.mu.Unlock()
	delete(slf.actors, lucId)
	return nil
}

func (slf *RemoteActorMgr) FindActor(lucId kkactor.LucencyID) (*RemoteActor, error) {
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
