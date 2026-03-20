package kkdiscovery

// Member 实现IMember接口的成员结构
type Member struct {
	nodeID     string // 节点ID, 用于标识一个节点。世界唯一。
	nodeType   string // 节点类型, 用于标识一个节点的类型。如：gate、game、login等。
	address    string // 网关transport地址, 网关与逻辑服之间的转发通道地址。
	rpcAddress string // actor通信rpc server地址, 用于actor与actor之间的通信。
	weight     int    //权重，用于负载均衡
	status     int    //状态（NodeStatusOnline或NodeStatusOffline）
}

var _ IMember = (*Member)(nil)

// GetNodeID 获取节点ID
func (m *Member) GetNodeID() string {
	return m.nodeID
}

// GetNodeType 获取节点类型
func (m *Member) GetNodeType() string {
	return m.nodeType
}

// GetAddress 获取网关transport地址，网关与逻辑服之间的转发通道地址。
func (m *Member) GetAddress() string {
	return m.address
}

// GetRpcAddress 获取actor通信rpc server地址, 用于actor与actor之间的通信。
func (m *Member) GetRpcAddress() string {
	return m.rpcAddress
}

// GetWeight 获取权重
func (m *Member) GetWeight() int {
	return m.weight
}

// GetStatus 获取状态
func (m *Member) GetStatus() int {
	return m.status
}
