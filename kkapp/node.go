package kkapp

import "github.com/vvisun/kkdg/utils/kklog"

// NewNodeInfo 创建节点信息
// 一般在启动时，从配置文件中读取节点信息并创建节点信息。
//  注意：nodeId 和 nodeType 是必须的。其他都是可选的。
func NewNodeInfo(nodeId, nodeType, address, rpcAddress string, settings map[string]string) *NodeInfo {
	if checkNodeID(nodeId) != nil {
		kklog.PanicLog("invalid node id") //一般都是启动时配置节点信息。非法id直接panic，避免影响后续逻辑
	}
	if checkNodeType(nodeType) != nil {
		kklog.PanicLog("invalid node type") //一般都是启动时配置节点信息。非法类型直接panic，避免影响正常启动。
	}
	if !IsValidActorNodeId(nodeId) {
		kklog.PanicLog("invalid node id") //一般都是启动时配置节点信息。非法id直接panic，避免影响后续逻辑
	}
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

// GetSettings 获取节点配置参数, 可以为空。
//  注意：由于大部分场景都是只读取数据，故这里返回的是引用。外部如果需要修改，需要自己复制一份。
func (slf *NodeInfo) GetSettings() map[string]string {
	return slf.settings
}
