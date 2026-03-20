package hubtcp

import (
	"github.com/vvisun/kkdg/kkapp/kkactor/actorhub"
	"github.com/vvisun/kkdg/kknet"
)

// HubServer。基于kktcp实现的注册中心。
type HubServer struct {
	srv            kknet.IServer
	remoteActorMgr *actorhub.RemoteActorMgr
	opts           Options
}
