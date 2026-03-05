package kkapp

const (
	NodeTypeGate  = "gate"  // 网关服
	NodeTypeLogic = "logic" // 业务服
)

const (
	SubEventNodeWeight = "EventNodeWeight" // 节点权重事件
)

type EventNodeWeight struct {
	NodeID string
	Weight int // 权重（目前直接用在线连接数作为权重）
	Status int // 状态（NodeStatusOnline或NodeStatusOffline）
}
