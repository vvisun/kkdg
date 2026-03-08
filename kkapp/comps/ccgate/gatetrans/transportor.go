package gatetrans

import "github.com/vvisun/kkdg/kknet"

// ITransportor 数据转发器接口。
// 抽象化接口，方便切换实现逻辑（如：使用Actor、使用Nats、使用RPC等）。
type ITransportor interface {
	// ForwardToLogic forwards a client message to logic side.
	// @param packet is a full stream packet [length,message]
	ForwardToLogic(sessionID string, packet []byte, logicNodeId string) error
	// ForwardToClient forwards a logic message to client side.
	// @param packet is a full stream packet [length,message]
	ForwardToClient(sessionID string, packet []byte) error
	// ForwardToClients forwards a logic message to multiple clients side.
	// @param packet is a full stream packet [length,message]
	ForwardToClients(sessionIDs []string, packet []byte) error
	// NotifyClientDisconnect notifies a client disconnect.
	NotifyClientDisconnect(sessionID string, logicNodeId string, connId kknet.CONN_ID) error
}
