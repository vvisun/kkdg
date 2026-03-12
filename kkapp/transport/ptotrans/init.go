package ptotrans

import (
	"sync"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/remotes/kkrpc"
)

var (
	initOnce sync.Once
)

func InitRpcMsgs(methodMgr *kkrpc.MethodManager) {
	kkrpc.RegisterOneWayMethod[RpcMsgRegister]("register", methodMgr)
	kkrpc.RegisterOneWayMethod[RpcS2Client]("s2c", methodMgr)
	kkrpc.RegisterOneWayMethod[RpcS2Clients]("s2cs", methodMgr)
	kkrpc.RegisterOneWayMethod[RpcC2S]("c2s", methodMgr)
	kkrpc.RegisterOneWayMethod[RpcClientDisconnect]("clientDisconnect", methodMgr)
}

func InitShardMsgs() {
	initOnce.Do(func() {
		initShardMsgs()
	})
}

func initShardMsgs() {
	kkapp.GetTransMsgPacket().GetRouter().Register(1, &RpcMsgRegister{}, "logic")
	kkapp.GetTransMsgPacket().GetRouter().Register(2, &RpcS2Client{}, "logic")
	kkapp.GetTransMsgPacket().GetRouter().Register(3, &RpcS2Clients{}, "logic")
	kkapp.GetTransMsgPacket().GetRouter().Register(4, &RpcC2S{}, "logic")
	kkapp.GetTransMsgPacket().GetRouter().Register(5, &RpcClientDisconnect{}, "logic")
}
