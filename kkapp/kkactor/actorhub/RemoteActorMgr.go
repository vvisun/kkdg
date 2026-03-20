package actorhub

import (
	"sync"

	"github.com/vvisun/kkdg/kkapp/kkactor"
)

// RemoteActor 远程Actor信息。
type RemoteActor struct {
	actorID kkactor.LucencyActorID // ActorID
	rpcAddr string                 // 远程RPC地址
}

// RemoteActorMgr 远程Actor管理器。
//
//	hubserver 与 hubclient 可以通过这个管理器缓存远程Actor信息。
type RemoteActorMgr struct {
	actors map[string]*RemoteActor
	mu     sync.RWMutex
}
