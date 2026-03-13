package kkcluster

import (
	"github.com/vvisun/kkdg/utils/kkpool"
)

type (
	// ClusterPacket 集群消息包
	ClusterPacket struct {
		BuildTime  int64
		Timeout    int64
		SourcePath string
		TargetPath string
		Sid        string
		FuncName   string
		ArgBytes   []byte
	}
)

type (
	// ClusterRequest 集群请求消息
	ClusterRequest struct {
		RequestID    string
		SourceNodeID string
		Packet       *ClusterPacket
	}

	// ClusterResponse 集群响应消息
	ClusterResponse struct {
		RequestID string
		Code      int32
		Data      []byte
	}
)

//----------------------------------------------------------

var (
	emptyClusterPacket = ClusterPacket{}
	gClusterPacketPool = kkpool.NewSfxPool(func() *ClusterPacket { return &ClusterPacket{} })
)

func NewClusterPacket() *ClusterPacket {
	return gClusterPacketPool.Get()
}

func PutClusterPacket(packet *ClusterPacket) {
	if packet == nil {
		return
	}
	*packet = emptyClusterPacket // 清空数据
	gClusterPacketPool.Put(packet)
}
