package ptotrans

import (
	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/remotes/kkrpc"
)

func init() {
	kkrpc.RegisterOneWayMethod[RpcMsgRegister]("register")
	kkrpc.RegisterOneWayMethod[RpcS2Client]("s2c")
	kkrpc.RegisterOneWayMethod[RpcS2Clients]("s2cs")
	kkrpc.RegisterOneWayMethod[RpcC2S]("c2s")
	kkrpc.RegisterOneWayMethod[RpcClientDisconnect]("clientDisconnect")

	kkapp.GetTransMsgPacket().GetRouter().Register(1, &RpcMsgRegister{}, "logic")
	kkapp.GetTransMsgPacket().GetRouter().Register(2, &RpcS2Client{}, "logic")
	kkapp.GetTransMsgPacket().GetRouter().Register(3, &RpcS2Clients{}, "logic")
	kkapp.GetTransMsgPacket().GetRouter().Register(4, &RpcC2S{}, "logic")
	kkapp.GetTransMsgPacket().GetRouter().Register(5, &RpcClientDisconnect{}, "logic")
}
