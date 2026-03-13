package kkcluster

import (
	"github.com/vvisun/kkdg/proto/pbcluster/fbtcluster"
	"github.com/vvisun/kkdg/utils/kkpool"
)

type (
	// ClusterPacket 集群消息包
	ClusterPacket = fbtcluster.ClusterPacket
)

type (
	// ClusterRequest 集群请求消息
	ClusterRequest = fbtcluster.ClusterRequest

	// ClusterResponse 集群响应消息
	ClusterResponse = fbtcluster.ClusterResponse
)

var gClusterPacketPool = kkpool.NewSfxPool(func() *ClusterPacket { return &ClusterPacket{} })

func NewClusterPacket() *ClusterPacket {
	return gClusterPacketPool.Get()
}

var emptyClusterPacket = ClusterPacket{}

func PutClusterPacket(packet *ClusterPacket) {
	if packet == nil {
		return
	}
	*packet = emptyClusterPacket // 清空数据
	gClusterPacketPool.Put(packet)
}
