package kkcluster

import (
	"time"
)

type (
	// 向其他节点发送消息，oneway, 没有response
	FunPublishHandler func(nodeID string, packet *ClusterPacket)
	// 向其他节点发送请求，request, 有response
	FunRequestHandler func(req *ClusterRequest) (*ClusterResponse, error)
)

// ICluster 集群接口
type ICluster interface {
	// 启动
	Start() error
	// 停止
	Stop()

	// 向指定节点发布消息
	Send(nodeID string, packet *ClusterPacket) error
	// 向同类型节点发布消息
	PublishType(nodeType string, packet *ClusterPacket) error
	// 向指定节点发送请求，有response，同步阻塞
	Request(nodeID string, packet *ClusterPacket, timeout ...time.Duration) ([]byte, ClusterErrorCode)
	// 向指定节点发送请求，有response，异步不阻塞
	RequestAsync(nodeID string, packet *ClusterPacket, callback func(data []byte, errCode ClusterErrorCode), timeout ...time.Duration) error

	// 设置发布消息处理器
	SetPublishHandler(handler FunPublishHandler)
	// 设置请求处理器
	SetRequestHandler(handler FunRequestHandler)

	// 获取统计信息
	Stats() ClusterStatsSnapshot
}
