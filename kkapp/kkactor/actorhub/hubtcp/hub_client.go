package hubtcp

import (
	"github.com/vvisun/kkdg/kkapp/kkactor/actorhub"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
)

// HubClient。基于kktcp实现的注册中心客户端。
type HubClient struct {
	clients        []kknet.IClient
	remoteActorMgr *actorhub.RemoteActorMgr
	opts           Options
	discovery      kkdiscovery.IDiscovery
}
