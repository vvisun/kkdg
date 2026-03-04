package transface

// ITransportor 数据转发器接口。
// 抽象化接口，方便切换实现逻辑（如：使用Actor、使用Nats、使用RPC等）。
type ITransportor interface {
	// ForwardToLogic forwards a client message to logic side.
	ForwardToLogic(sessionID string, msgBytes []byte, logicNodeId string) error
	// ForwardToClient forwards a logic message to client side.
	ForwardToClient(sessionID string, msgBytes []byte) error
	// GetSessionMgr gets the session manager
	GetSessionMgr() ISessionManager
}
