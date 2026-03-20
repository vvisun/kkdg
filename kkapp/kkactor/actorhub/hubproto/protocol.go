package hubproto

import (
	"github.com/vvisun/kkdg/kkapp/kkactor/actorremotes"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

var HubMessagePacket = kkpacket.NewFullPacket(
	kkpacket.DefaultStreamPacket(),
	kkpacket.NewMessagePacket(
		kkpacket.NewPacketHeadWithNames(
			[]kkpacket.IHeadPart{
				&kkpacket.PartUint16{},
			},
			[]string{
				kkpacket.PartNameMsgID, // 消息ID
			},
		),
		kkcodec.GetCodec(kkcodec.CodecTypeJson),
		kkpacket.NewMsgRouter(),
	),
)

const (
	MsgIDRegisterNodeReq uint16 = 1 + iota
	MsgIDRegisterNodeResp
	MsgIDRegisterActorReq
	MsgIDRegisterActorResp
	MsgIDFindActorReq
	MsgIDFindActorResp
	MsgIDGetAllActorsOfNodeReq
	MsgIDGetAllActorsOfNodeResp
)

type (
	// RegisterNodeReq 注册|更新|注销节点请求
	// 相当于作为discovery的MemberInfo。
	// 作为连接hub server的第一个请求，只有Password鉴权通过的节点才能继续后续的协议通信。
	RegisterNodeReq struct {
		OpCode     int                    `json:"opCode"`     // 操作码，1表示注册，2表示更新，3表示注销。
		Password   string                 `json:"password"`   // 鉴权密码，用于中心服鉴权。需要与hub server的鉴权密码相同才能通过注册。
		MemberInfo kkdiscovery.MemberInfo `json:"memberInfo"` // 节点信息，用于注册或更新。
	}
	// RegisterNodeResp 注册节点响应
	RegisterNodeResp struct {
		Code    int    `json:"code"`    // 错误码，0表示成功，其他表示失败
		Message string `json:"message"` // 错误信息
	}

	// RegisterActorReq 注册actor请求
	RegisterActorReq struct {
		OpCode  int                   `json:"opCode"`  // 操作码，1表示注册，2表示注销。
		ActorID actorremotes.ActorRef `json:"actorID"` // actor透明ID，用于注册或更新。
	}
	RegisterActorResp struct {
		Code    int                   `json:"code"`    // 错误码，0表示成功，其他表示失败
		Message string                `json:"message"` // 错误信息
		ActorID actorremotes.ActorRef `json:"actorID"` // RegisterActorRequest中的actorID。
	}

	// FindActorReq 寻找actor请求
	FindActorReq struct {
		OpCode   int    `json:"opCode"`   // 操作码，1表示寻找，2表示注销。
		ActorKey string `json:"actorKey"` // actor标识，用于寻找。
	}
	FindActorResp struct {
		Code     int                    `json:"code"`     // 错误码，0表示成功，其他表示失败
		Message  string                 `json:"message"`  // 错误信息
		ActorID  actorremotes.ActorRef  `json:"actorID"`  // 找到的actorID。
		NodeInfo kkdiscovery.MemberInfo `json:"nodeInfo"` // 找到的actor所在节点信息。
	}

	// GetAllActorsOfNodeReq 获取某个节点上的所有actor列表请求
	GetAllActorsOfNodeReq struct {
		NodeID string `json:"nodeID"` // 节点ID，用于获取。
	}
	GetAllActorsOfNodeResp struct {
		Code    int                      `json:"code"`    // 错误码，0表示成功，其他表示失败
		Message string                   `json:"message"` // 错误信息
		NodeID  string                   `json:"nodeID"`  // 节点ID。
		Actors  []*actorremotes.ActorRef `json:"actors"`  // 节点上的所有actor列表。
	}
)
