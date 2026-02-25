package kkdiscovery

// Member 实现IMember接口的成员结构
type Member struct {
	nodeID   string
	nodeType string
	address  string //rpc server address
	weight   int    //权重，用于负载均衡
	settings map[string]string
}

var _ IMember = (*Member)(nil)

// NewMember 创建新的成员
func NewMember(nodeID, nodeType, address string, weight int, settings map[string]string) *Member {
	return &Member{
		nodeID:   nodeID,
		nodeType: nodeType,
		address:  address,
		weight:   weight,
		settings: settings,
	}
}

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

// GetWeight 获取权重
func (m *Member) GetWeight() int {
	return m.weight
}

// SetWeight 设置权重
func (m *Member) SetWeight(weight int) {
	m.weight = weight
}

// GetSetting 获取设置
func (m *Member) GetSetting(k string) (string, bool) {
	if m.settings == nil {
		return "", false
	}
	value, ok := m.settings[k]
	return value, ok
}
