package kkapp

const (
	// 节点ID最大长度
	MaxNodeIDLength int = 24
	// 节点类型最大长度
	MaxNodeTypeLength int = 24
)

// INodeIdentity 节点身份接口
type INodeIdentity interface {
	GetNodeId() string   // 获取节点ID
	GetNodeType() string // 获取节点类型
}

// NewNodeInfo 创建节点信息
func NewNodeInfo(nodeId, nodeType, address, rpcAddress string, settings map[string]string) *NodeInfo {
	return &NodeInfo{
		nodeId:     nodeId,
		nodeType:   nodeType,
		address:    address,
		rpcAddress: rpcAddress,
		settings:   settings,
	}
}

// NodeInfo 节点信息
type NodeInfo struct {
	nodeId     string            // 节点ID。全局唯一。
	nodeType   string            // 节点类型。如：gate、game、login等
	address    string            // 节点地址。如：127.0.0.1:8080
	rpcAddress string            // rpc地址。如：127.0.0.1:8080
	settings   map[string]string // 节点配置参数。如：{"log_level": "debug"}
}

var _ INodeIdentity = (*NodeInfo)(nil)

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

func (slf *NodeInfo) GetSetting(k string) (string, bool) {
	if slf.settings == nil {
		return "", false
	}
	value, ok := slf.settings[k]
	return value, ok
}
