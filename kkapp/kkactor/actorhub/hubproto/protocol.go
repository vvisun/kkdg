package hubproto

import (
	"github.com/vvisun/kkdg/kkapp/kkactor/actorremotes"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
)

const (
	MsgIDRegisterActorReq kkpacket.MSGID = 1 + iota
	MsgIDRegisterActorResp
	MsgIDFindActorReq
	MsgIDFindActorResp
	MsgIDGetAllActorsOfNodeReq
	MsgIDGetAllActorsOfNodeResp
	MsgIDErrorResp
)

type (
	// 统一错误响应
	//  比如RegisterActorReq请求处理失败，则返回ErrorResp，
	//  其中ReqID为RegisterActorReq中的ReqID，Code为错误码，Message为错误信息。
	//  其他请求类似。
	ErrorResp struct {
		ReqID   uint64 `json:"reqID"`   // 各请求里带过来的ReqID
		Code    int    `json:"code"`    // 错误码
		Message string `json:"message"` // 错误信息
	}

	// RegisterActorReq 请求注册|注销actor
	RegisterActorReq struct {
		ReqID   uint64                `json:"reqID"`   // 请求ID，用于请求与响应的匹配。
		OpCode  int                   `json:"opCode"`  // 操作码，1表示注册，2表示注销。
		ActorID actorremotes.ActorRef `json:"actorID"` // actor透明ID，用于注册或更新。
	}
	RegisterActorResp struct {
		ReqID   uint64                `json:"reqID"`   // 请求ID，用于请求与响应的匹配。
		OpCode  int                   `json:"opCode"`  // 操作码，1表示注册，2表示注销。
		ActorID actorremotes.ActorRef `json:"actorID"` // RegisterActorRequest中的actorID。
	}

	// FindActorReq 请求寻找actor
	FindActorReq struct {
		ReqID   uint64                `json:"reqID"`   // 请求ID，用于请求与响应的匹配。
		ActorID actorremotes.ActorRef `json:"actorID"` // 与 LucencyActorID 一致。
	}
	FindActorResp struct {
		ReqID    uint64                 `json:"reqID"`    // 请求ID，用于请求与响应的匹配。
		ActorID  actorremotes.ActorRef  `json:"actorID"`  // 找到的actorID。
		NodeInfo kkdiscovery.MemberInfo `json:"nodeInfo"` // 找到的actor所在节点信息。
	}

	// GetAllActorsOfNodeReq 请求获取某个节点上的所有actor列表
	GetAllActorsOfNodeReq struct {
		ReqID  uint64 `json:"reqID"`  // 请求ID，用于请求与响应的匹配。
		NodeID string `json:"nodeID"` // 节点ID，用于获取。
	}
	GetAllActorsOfNodeResp struct {
		ReqID    uint64                   `json:"reqID"`    // 请求ID，用于请求与响应的匹配。
		NodeInfo kkdiscovery.MemberInfo   `json:"nodeInfo"` // 节点信息。
		Actors   []*actorremotes.ActorRef `json:"actors"`   // 节点上的所有actor列表。
	}
)
