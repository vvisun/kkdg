package kkapp

const (
	// 节点ID最大长度
	MaxNodeIDLength int = 24
	// 节点类型最大长度
	MaxNodeTypeLength int = 24
)

func NewNodeInfo(nodeId, nodeType, address, rpcAddress string, enabled bool, settings map[string]string) *NodeInfo {
	return &NodeInfo{
		nodeId:     nodeId,
		nodeType:   nodeType,
		address:    address,
		rpcAddress: rpcAddress,
		enabled:    enabled,
		settings:   settings,
	}
}

type NodeInfo struct {
	nodeId     string
	nodeType   string
	address    string
	rpcAddress string
	enabled    bool
	settings   map[string]string
}

func (slf *NodeInfo) GetNodeId() string {
	return slf.nodeId
}

func (slf *NodeInfo) GetNodeType() string {
	return slf.nodeType
}

func (slf *NodeInfo) GetAddress() string {
	return slf.address
}

func (slf *NodeInfo) GetRpcAddress() string {
	return slf.rpcAddress
}

func (slf *NodeInfo) GetEnabled() bool {
	return slf.enabled
}

func (slf *NodeInfo) GetSetting(k string) (string, bool) {
	if slf.settings == nil {
		return "", false
	}
	value, ok := slf.settings[k]
	return value, ok
}
