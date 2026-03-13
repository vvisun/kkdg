package gatetrans

import (
	"github.com/vvisun/kkdg/kknet"
)

// ITransportor 数据转发器接口。
// 抽象化接口，方便切换实现逻辑（如：使用Actor、使用Nats、使用RPC等）。
type ITransportor interface {
	Stop() error
	// ForwardToLogic forwards a client message to logic side.
	//  @param packet is a full stream packet [length,message]
	ForwardToLogic(sessionID string, packet []byte, logicNodeId string) error
	// ForwardToClient forwards a logic message to client side.
	//  @param packet is a full stream packet [length,message]
	ForwardToClient(sessionID string, packet []byte) error
	// ForwardToClients forwards a logic message to multiple clients side.
	//  @param packet is a full stream packet [length,message]
	ForwardToClients(sessionIDs []string, packet []byte) error
	// NotifyClientDisconnect notifies a client disconnect.
	NotifyClientDisconnect(sessionID string, logicNodeId string, connId kknet.CONN_ID) error
	// NotifyClientConnect notifies a client connect.
	NotifyClientConnect(sessionID string, logicNodeId string, connId kknet.CONN_ID) error
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
)
