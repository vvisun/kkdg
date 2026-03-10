package kkapp

import (
	"strings"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kklog"
)

const (
	// 节点ID最大长度
	maxNodeIDLength int = 16
	// 节点类型最大长度
	maxNodeTypeLength int = 16
)

// nodeId与actorKey的分隔符
const ActorKeySeparator = "/"

// checkNodeID 检查节点ID是否有效
func checkNodeID(nodeId string) error {
	if len(nodeId) < 1 {
		kklog.Errorf("invalid node id: %s", nodeId)
		return kkerrors.ErrInvalidNodeID
	}
	if len(nodeId) > maxNodeIDLength {
		kklog.Errorf("node id is too long: %s", nodeId)
		return kkerrors.ErrInvalidNodeID
	}
	if strings.Contains(nodeId, ActorKeySeparator) {
		kklog.Errorf("node id contains actor key separator: %s", nodeId)
		return kkerrors.ErrInvalidNodeID
	}
	return nil
}

// checkNodeType 检查节点类型是否有效
func checkNodeType(nodeType string) error {
	if len(nodeType) < 1 {
		kklog.Errorf("invalid node type: %s", nodeType)
		return kkerrors.ErrInvalidNodeType
	}
	if len(nodeType) > maxNodeTypeLength {
		kklog.Errorf("node type is too long: %s", nodeType)
		return kkerrors.ErrInvalidNodeType
	}
	if strings.Contains(nodeType, ActorKeySeparator) {
		kklog.Errorf("node type contains actor key separator: %s", nodeType)
		return kkerrors.ErrInvalidNodeType
	}
	return nil
}

// NewNodeInfo 创建节点信息
// 一般在启动时，从配置文件中读取节点信息并创建节点信息。
func NewNodeInfo(nodeId, nodeType, address, rpcAddress string, settings map[string]string) *NodeInfo {
	if checkNodeID(nodeId) != nil {
		panic("invalid node id")
	}
	if checkNodeType(nodeType) != nil {
		panic("invalid node type")
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
