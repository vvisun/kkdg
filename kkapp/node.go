package kkapp

import (
	"github.com/vvisun/kkdg/kkapp/achecker"
	"github.com/vvisun/kkdg/utils/kklog"
)

// NewNodeInfo 创建节点信息
// 一般在启动时，从配置文件中读取节点信息并创建节点信息。
//
//	 注意：nodeId 和 nodeType 是必须的。其他都是可选的。
//		 @param nodeId 节点ID。全局唯一。
//		 @param nodeType 节点类型。如：gate、game、login等
//		 @param address 可选。网关transport地址, 网关与逻辑服之间的转发通道地址。服务发现时使用。
//		 @param rpcAddress 可选。actor通信rpc server地址, 用于actor与actor之间的通信。服务发现时使用。
//		 @return 节点信息
func NewNodeInfo(nodeId, nodeType, address, rpcAddress string) *NodeInfo {
	if achecker.CheckNodeID(nodeId) != nil {
		kklog.PanicLog("invalid node id") //一般都是启动时配置节点信息。非法id直接panic，避免影响后续逻辑
	}
	if achecker.CheckNodeType(nodeType) != nil {
		kklog.PanicLog("invalid node type") //一般都是启动时配置节点信息。非法类型直接panic，避免影响正常启动。
	}
	if !achecker.IsValidActorNodeId(nodeId) {
		kklog.PanicLog("invalid node id") //一般都是启动时配置节点信息。非法id直接panic，避免影响后续逻辑
	}
	return &NodeInfo{
		nodeId:     nodeId,
		nodeType:   nodeType,
		address:    address,
		rpcAddress: rpcAddress,
	}
}

// NodeInfo 节点信息
type NodeInfo struct {
	nodeId     string // 节点ID。全局唯一。
	nodeType   string // 节点类型。如：gate、game、login等
	address    string // 网关transport地址, 网关与逻辑服之间的转发通道地址。服务发现时使用。
	rpcAddress string // actor通信rpc server地址, 用于actor与actor之间的通信。服务发现时使用。
}

var _ INodeIdentity = (*NodeInfo)(nil)

// GetNodeId 获取节点ID。全局唯一。
func (slf *NodeInfo) GetNodeId() string {
	return slf.nodeId
}

// GetNodeType 获取节点类型。如：gate、game、login等
func (slf *NodeInfo) GetNodeType() string {
	return slf.nodeType
}

// GetAddress 获取网关transport地址, 网关与逻辑服之间的转发通道地址。
func (slf *NodeInfo) GetAddress() string {
	return slf.address
}

// GetRpcAddress 获取actor通信rpc server地址, 用于actor与actor之间的通信。
func (slf *NodeInfo) GetRpcAddress() string {
	return slf.rpcAddress
}
