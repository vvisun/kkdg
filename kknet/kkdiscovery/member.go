package kkdiscovery

// Member 实现IMember接口的成员结构
type Member struct {
	nodeID   string
	nodeType string
	address  string //rpc server address
	settings map[string]string
}

var _ IMember = (*Member)(nil)

// NewMember 创建新的成员
func NewMember(nodeID, nodeType, address string, settings map[string]string) *Member {
	return &Member{
		nodeID:   nodeID,
		nodeType: nodeType,
		address:  address,
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

// GetSetting 获取设置
func (m *Member) GetSetting(k string) (string, bool) {
	if m.settings == nil {
		return "", false
	}
	value, ok := m.settings[k]
	return value, ok
}
