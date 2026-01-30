package ccgate

// ITransportor 数据转发器接口。
// 抽象化接口，方便切换实现逻辑（如：使用Actor、使用Nats、使用RPC等）。
type ITransportor interface {
	// ForwardToLogic forwards a client message to logic side.
	ForwardToLogic(sessionID string, msgRoute string, msgBytes []byte) error
}
