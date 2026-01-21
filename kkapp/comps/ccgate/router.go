package ccgate

import (
	"sync"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkactor"
)

// Router is a router for the gate.
// 客户端 -> 网关 -> 业务服
// 业务服 -> 网关 -> 客户端
type Router struct {
	mu       sync.RWMutex
	connMgr  kknet.IConnManager            // connection manager
	pidCache map[kknet.CONN_ID]kkactor.PID // pid cache
}
