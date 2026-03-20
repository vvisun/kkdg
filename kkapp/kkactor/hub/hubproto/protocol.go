package hubproto

import (
	"github.com/vvisun/kkdg/kkapp/kkactor/transport/actorremotes"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
)

const (
	zero kkpacket.MSGID = iota
	MsgIDAuthReq
	MsgIDAuthResp
	MsgIDRegisterActorReq
	MsgIDRegisterActorResp
	MsgIDFindActorReq
	MsgIDFindActorResp
	MsgIDGetAllActorsOfNodeReq
	MsgIDGetAllActorsOfNodeResp
)

type (
	// 统一错误响应
	ErrorResp struct {
		Code    ErrorCode `json:"code"`    // 错误码
		Message string    `json:"message"` // 错误信息
	}

	// 认证请求
	AuthReq struct {
		Password string `json:"password"` // 密码，用于请求与响应的匹配。
	}
	AuthResp struct {
		Code    ErrorCode `json:"code"`    // 错误码
		Message string    `json:"message"` // 错误信息
	}

	// RegisterActorReq 请求注册|注销actor
	RegisterActorReq struct {
		ReqID    uint64                  `json:"reqID"`    // 请求ID，用于请求与响应的匹配。
		OpCode   int                     `json:"opCode"`   // 操作码，1表示注册，2表示注销。
		ActorID  actorremotes.ActorRef   `json:"actorID"`  // actor透明ID，用于注册或更新。
		NodeInfo *kkdiscovery.MemberInfo `json:"nodeInfo"` // actor所在节点信息。
	}
	RegisterActorResp struct {
		ReqID     uint64                `json:"reqID"`     // 请求ID，用于请求与响应的匹配。
		OpCode    int                   `json:"opCode"`    // 操作码，1表示注册，2表示注销。
		ActorID   actorremotes.ActorRef `json:"actorID"`   // RegisterActorRequest中的actorID。
		ErrorInfo *ErrorResp            `json:"errorInfo"` // 错误信息，如果请求处理失败，则返回错误信息。
	}

	// FindActorReq 请求寻找actor
	FindActorReq struct {
		ReqID   uint64                `json:"reqID"`   // 请求ID，用于请求与响应的匹配。
		ActorID actorremotes.ActorRef `json:"actorID"` // 与 kkactor.LucencyID 语义一致（NodeID + ActorKey）。
	}
	FindActorResp struct {
		ReqID     uint64                  `json:"reqID"`     // 请求ID，用于请求与响应的匹配。
		ActorID   actorremotes.ActorRef   `json:"actorID"`   // 找到的actorID。
		NodeInfo  *kkdiscovery.MemberInfo `json:"nodeInfo"`  // 找到的actor所在节点信息。
		ErrorInfo *ErrorResp              `json:"errorInfo"` // 错误信息，如果请求处理失败，则返回错误信息。
	}

	// GetAllActorsOfNodeReq 请求获取某个节点上的所有actor列表
	GetAllActorsOfNodeReq struct {
		ReqID  uint64 `json:"reqID"`  // 请求ID，用于请求与响应的匹配。
		NodeID string `json:"nodeID"` // 节点ID，用于获取。
	}
	GetAllActorsOfNodeResp struct {
		ReqID     uint64                   `json:"reqID"`     // 请求ID，用于请求与响应的匹配。
		NodeInfo  *kkdiscovery.MemberInfo  `json:"nodeInfo"`  // 节点信息。
		Actors    []*actorremotes.ActorRef `json:"actors"`    // 节点上的所有actor列表。
		ErrorInfo *ErrorResp               `json:"errorInfo"` // 错误信息，如果请求处理失败，则返回错误信息。
	}
)
