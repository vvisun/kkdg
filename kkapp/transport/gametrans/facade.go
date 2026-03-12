package gametrans

// ITransportor 数据转发器接口。
// 抽象化接口，方便切换实现逻辑（如：使用Actor、使用Nats、使用RPC等）。
type ITransportor interface {
	Stop() error
	// ForwardToClient forwards a message to a client.
	// @param packet is a full stream packet [length,message]
	ForwardToClient(sessionID string, packet []byte) error
	// ForwardToClients forwards a message to multiple clients.
	// @param packet is a full stream packet [length,message]
	ForwardToClients(sessionIDs []string, packet []byte) error
	// SendToClient sends a message to a client.
	SendToClient(sessionID string, msg any) error
	// SendToClients sends a message to multiple clients.
	SendToClients(sessionIDs []string, msg any) error
}
