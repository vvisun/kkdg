package ptotrans

import (
	"sync"

	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/remotes/kkrpc"
)

var (
	initShardOnce sync.Once
)

func InitRpcMsgs(methodMgr *kkrpc.MethodManager) {
	kkrpc.RegisterOneWayMethod[RpcMsgRegister](methodMgr)
	kkrpc.RegisterOneWayMethod[RpcS2Client](methodMgr)
	kkrpc.RegisterOneWayMethod[RpcS2Clients](methodMgr)
	kkrpc.RegisterOneWayMethod[RpcC2S](methodMgr)
	kkrpc.RegisterOneWayMethod[RpcClientDisconnect](methodMgr)
	kkrpc.RegisterOneWayMethod[RpcAllocClient](methodMgr)
	kkrpc.RegisterOneWayMethod[RpcClientLoginLogout](methodMgr)
}

func InitShardMsgs(router *kkpacket.MsgRouter) {
	initShardOnce.Do(func() {
		initShardMsgs(router)
	})
}

func initShardMsgs(router *kkpacket.MsgRouter) {
	router.Register(1, &RpcMsgRegister{}, "logic")
	router.Register(2, &RpcS2Client{}, "logic")
	router.Register(3, &RpcS2Clients{}, "logic")
	router.Register(4, &RpcC2S{}, "logic")
	router.Register(5, &RpcClientDisconnect{}, "logic")
	router.Register(6, &RpcAllocClient{}, "logic")
	router.Register(7, &RpcClientLoginLogout{}, "logic")
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
