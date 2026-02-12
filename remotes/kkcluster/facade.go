package kkcluster

import (
	"time"
)

// ICluster 集群接口
type ICluster interface {
	// 初始化
	Init() error
	// 停止
	Stop()

	// 向指定节点发布消息
	PublishRemote(nodeID string, packet *ClusterPacket) error
	// 向同类型节点发布消息
	PublishRemoteType(nodeType string, packet *ClusterPacket) error
	// 向指定节点发送请求，有response
	RequestRemote(nodeID string, packet *ClusterPacket, timeout ...time.Duration) ([]byte, ClusterErrorCode)

	// 设置发布消息处理器
	SetPublishHandler(handler FunPublishHandler)
	// 设置请求处理器
	SetRequestHandler(handler FunRequestHandler)

	// 获取统计信息
	Stats() ClusterStatsSnapshot
}

type (
	// 向其他节点发送消消，no response
	FunPublishHandler func(nodeID string, packet *ClusterPacket)
	// 向其他节点发送请求，有response
	FunRequestHandler func(req *ClusterRequest) (*ClusterResponse, error)
)
