package ptotrans

import "github.com/vvisun/kkdg/kknet/kkpacket"

const (
	MsgIDRpcMsgRegister       kkpacket.MSGID = 1
	MsgIDRpcS2Client          kkpacket.MSGID = 2
	MsgIDRpcS2Clients         kkpacket.MSGID = 3
	MsgIDRpcC2S               kkpacket.MSGID = 4
	MsgIDRpcClientDisconnect  kkpacket.MSGID = 5
	MsgIDRpcAllocClient       kkpacket.MSGID = 6
	MsgIDRpcClientLoginLogout kkpacket.MSGID = 7
	MsgIDRpcUnregister        kkpacket.MSGID = 8
)

type (
	// MsgID: MsgIDRpcMsgRegister
	// 逻辑服注册到网关: 逻辑服->网关->逻辑服
	RpcMsgRegister struct {
		ShardIdx int
		NodeId   string
		NodeType string
	}

	// MsgID: MsgIDRpcS2Client
	// 网关转发消息到客户端: 逻辑服->网关->客户端
	RpcS2Client struct {
		ClientId string //sessionID
		Payload  []byte //整包[length,message] 原样转发
	}

	// MsgID: MsgIDRpcS2Clients
	// 网关转发消息到多个客户端: 逻辑服->网关->多个客户端
	RpcS2Clients struct {
		ClientIds []string //sessionID列表
		Payload   []byte   //整包[length,message] 原样转发
	}

	// MsgID: MsgIDRpcC2S
	// 网关转发客户端消息到逻辑服: 客户端->网关->逻辑服
	RpcC2S struct {
		ClientId   string //sessionID
		GateNodeId string //网关节点ID
		Payload    []byte //整包[length,message] 原样转发
	}

	// MsgID: MsgIDRpcClientDisconnect
	// 网关 -> 逻辑服：客户端断开事件。
	// 逻辑服的视角相当于连接到本逻辑服的某个客户端断开连接。
	RpcClientDisconnect struct {
		ClientId  string   //sessionID
		ClientIds []string //sessionID列表
	}

	// MsgID: MsgIDRpcAllocClient
	// 网关 -> 逻辑服：分配客户端到本逻辑服。
	// 逻辑服的视角相当于某个客户端连接到本逻辑服。
	RpcAllocClient struct {
		ClientId string //sessionID
	}

	// MsgID: MsgIDRpcClientLoginLogout
	// 逻辑服 -> 网关：客户端登入登出事件
	RpcClientLoginLogout struct {
		UserId     int64  //用户ID
		IsLogin    bool   //是否登录
		ClientId   string //sessionID
		NodeType   string //opt: 逻辑节点类型, 用于校验
		NodeId     string //opt: 逻辑节点ID, 用于校验
		GateNodeId string //opt: 网关节点ID, 用于校验
	}

	// MsgID: MsgIDRpcUnregister
	// 逻辑服 -> 网关：逻辑服注销事件
	RpcUnregister struct {
		NodeId string //逻辑节点ID
	}
)
