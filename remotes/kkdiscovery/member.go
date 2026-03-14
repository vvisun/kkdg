package kkdiscovery

// Member 实现IMember接口的成员结构
type Member struct {
	nodeID     string
	nodeType   string
	address    string // 监听地址
	rpcAddress string // rpc server address
	weight     int    //权重，用于负载均衡
	status     int    //状态（NodeStatusOnline或NodeStatusOffline）
	settings   map[string]string
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

// GetAddress 获取地址
func (m *Member) GetAddress() string {
	return m.address
}

// GetRpcAddress 获取rpc地址
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

// GetSetting 获取设置
func (m *Member) GetSetting(k string) (string, bool) {
	if m.settings == nil {
		return "", false
	}
	value, ok := m.settings[k]
	return value, ok
}
