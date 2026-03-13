package ptotrans

import (
	"sync"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/remotes/kkrpc"
)

var (
	initShardOnce sync.Once
)

func InitRpcMsgs(methodMgr *kkrpc.MethodManager) {
	kkrpc.RegisterOneWayMethod[RpcMsgRegister]("register", methodMgr)
	kkrpc.RegisterOneWayMethod[RpcS2Client]("s2c", methodMgr)
	kkrpc.RegisterOneWayMethod[RpcS2Clients]("s2cs", methodMgr)
	kkrpc.RegisterOneWayMethod[RpcC2S]("c2s", methodMgr)
	kkrpc.RegisterOneWayMethod[RpcClientDisconnect]("clientDisconnect", methodMgr)
	kkrpc.RegisterOneWayMethod[RpcAllocClient]("allocClient", methodMgr)
	kkrpc.RegisterOneWayMethod[RpcClientLoginLogout]("clientLoginLogout", methodMgr)
}

func InitShardMsgs() {
	initShardOnce.Do(func() {
		initShardMsgs()
	})
}

func initShardMsgs() {
	kkapp.GetTransMsgPacket().GetRouter().Register(1, &RpcMsgRegister{}, "logic")
	kkapp.GetTransMsgPacket().GetRouter().Register(2, &RpcS2Client{}, "logic")
	kkapp.GetTransMsgPacket().GetRouter().Register(3, &RpcS2Clients{}, "logic")
	kkapp.GetTransMsgPacket().GetRouter().Register(4, &RpcC2S{}, "logic")
	kkapp.GetTransMsgPacket().GetRouter().Register(5, &RpcClientDisconnect{}, "logic")
	kkapp.GetTransMsgPacket().GetRouter().Register(6, &RpcAllocClient{}, "logic")
	kkapp.GetTransMsgPacket().GetRouter().Register(7, &RpcClientLoginLogout{}, "logic")
}

// 网关与业务服之间的消息转发函数名。nats模式使用
const (
	// 客户端->网关->业务服的消息转发函数名
	FuncNameC2S = "c2s"
	// 业务服->网关->客户端的消息转发函数名
	FuncNameSendToClient = "1"
	// 业务服->网关->多个客户端的消息转发函数名
	FuncNameSendToClients = "N"
	// 网关 -> 业务服：客户端断开事件
	FuncNameClientDisconnect = "cliMiss"
	// 网关 -> 业务服：分配客户端到本业务服
	FuncNameAllocClient = "allocClient"
	// 逻辑服 -> 网关：客户端登入登出事件
	FuncNameClientLoginLogout = "clientLoginLogout"
)
