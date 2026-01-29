package ccgate

import "github.com/vvisun/kkdg/kknet"

// ITransportor 数据转发器接口
// 抽象化接口，方便切换实现逻辑（如：使用Actor、使用Nats、使用RPC等）
type ITransportor interface {
	// ClientToLogic 客户端 -> 网关 -> 业务服
	ClientToLogic(connID kknet.CONN_ID, data []byte) error
	// LogicToClient 业务服 -> 网关 -> 客户端
	LogicToClient(connID kknet.CONN_ID, data []byte) error
	// LogicToClients 业务服 -> 网关 -> 多个客户端
	LogicToClients(connIDs []kknet.CONN_ID, data []byte) error
}
