package ptotrans

import (
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/remotes/kkrpc"
)

func InitRpcMsgs(methodMgr *kkrpc.MethodManager) {
	kkrpc.RegisterOneWayMethod[RpcMsgRegister](methodMgr)
	kkrpc.RegisterOneWayMethod[RpcS2Client](methodMgr)
	kkrpc.RegisterOneWayMethod[RpcS2Clients](methodMgr)
	kkrpc.RegisterOneWayMethod[RpcC2S](methodMgr)
	kkrpc.RegisterOneWayMethod[RpcClientDisconnect](methodMgr)
	kkrpc.RegisterOneWayMethod[RpcAllocClient](methodMgr)
	kkrpc.RegisterOneWayMethod[RpcClientLoginLogout](methodMgr)
	kkrpc.RegisterOneWayMethod[RpcUnregister](methodMgr)
	kkrpc.RegisterOneWayMethod[RpcCloseClient](methodMgr)
}

func InitShardMsgs(router *kkpacket.MsgRouter) {
	router.Register(MsgIDRpcMsgRegister, &RpcMsgRegister{}, "logic")
	router.Register(MsgIDRpcS2Client, &RpcS2Client{}, "logic")
	router.Register(MsgIDRpcS2Clients, &RpcS2Clients{}, "logic")
	router.Register(MsgIDRpcC2S, &RpcC2S{}, "logic")
	router.Register(MsgIDRpcClientDisconnect, &RpcClientDisconnect{}, "logic")
	router.Register(MsgIDRpcAllocClient, &RpcAllocClient{}, "logic")
	router.Register(MsgIDRpcClientLoginLogout, &RpcClientLoginLogout{}, "logic")
	router.Register(MsgIDRpcUnregister, &RpcUnregister{}, "logic")
	router.Register(MsgIDRpcCloseClient, &RpcCloseClient{}, "logic")
}

// 网关与业务服之间的消息转发函数名。nats模式使用
const (
	// 逻辑服 -> 网关：逻辑服注册事件
	FuncNameRegister = "register"
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
	// 逻辑服 -> 网关：逻辑服注销事件
	FuncNameUnregister = "unregister"
	// 逻辑服 -> 网关：关闭客户端连接
	FuncNameCloseClient = "closeClient"
)
