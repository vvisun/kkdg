package hubtcp

import (
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
)

// Hub。基于kktcp实现的注册中心。
type Hub struct {
	addr   string
	stream kkpacket.IPacket
	srv    kknet.IServer
}
