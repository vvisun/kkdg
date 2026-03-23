package gatetrans

import (
	"github.com/vvisun/kkdg/kknet"
)

// ITransportor 数据转发器接口。
// 抽象化接口，方便切换实现逻辑（如：使用Actor、使用Nats、使用RPC等）。
type ITransportor interface {
	Stop() error
	// forwards a client message to logic side.
	// 转发客户端消息到逻辑服。
	//  @param packet is a full stream packet [length,message]
	ForwardToLogic(sessionID string, packet []byte, logicNodeId string) error
	// forwards a logic message to client side.
	// 转发逻辑服消息到单个客户端。
	//  @param packet is a full stream packet [length,message]
	ForwardToClient(sessionID string, packet []byte) error
	// forwards a logic message to multiple clients side.
	// 转发逻辑服消息到多个客户端。
	//  @param packet is a full stream packet [length,message]
	ForwardToClients(sessionIDs []string, packet []byte) error
	// notifies a client disconnect to logic side.
	// 通知逻辑服，网关处该客户端连接已断开
	NotifyClientDisconnect(sessionID string, logicNodeId string, connId kknet.CONN_ID) error
	// notifies a client connect to logic side. when client is allocated to the logic server.
	// 通知逻辑服，网关处该客户端连接已建立（分配到该逻辑服时）
	NotifyClientConnect(sessionID string, logicNodeId string, connId kknet.CONN_ID) error
	// hooks a message.
	// 钩子消息，用于处理网关收到消息后的回调。
	HookMsg(listener MsgHookListener)
}

// 简易版的discovery成员管理器接口。
type (
	// IMember 成员接口。used by gateway to choose logic server.
	IMember interface {
		GetNodeID() string
		GetNodeType() string
	}
	// IMemberMgr 成员管理器接口。used by gateway to manage logic servers.
	IMemberMgr interface {
		// 遍历成员, fn返回false时停止遍历
		Range(fn func(nodeId string, member IMember) bool)
	}
	//
	IMemberMgrGetter interface {
		GetMemberMgr() IMemberMgr
	}
)
