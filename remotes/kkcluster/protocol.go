package kkcluster

import (
	"github.com/vvisun/kkdg/utils/kkpool"
)

type (
	// ClusterPacket 集群消息包
	ClusterPacket struct {
		BuildTime  int64  // 构建时间（毫秒）
		Timeout    int64  // 函数请求超时时间(毫秒)
		SourcePath string // 源节点路径
		TargetPath string // 目标节点路径
		Sid        string // session unique id
		FuncName   string // 函数名|methodID|msgID
		ArgBytes   []byte // 函数参数|Payload
	}
)

type (
	// ClusterRequest 集群请求消息
	ClusterRequest struct {
		RequestID    string // 请求id（自动生成）
		SourceNodeID string // 源节点id
		Packet       *ClusterPacket
	}

	// ClusterResponse 集群响应消息
	ClusterResponse struct {
		RequestID string // 请求id（与请求消息的requestID相同，eg: 收到req, resp时 resp.RequestID = req.RequestID）
		Code      int32  // 错误码（ClusterErrorCode），0表示成功，其他表示失败
		Data      []byte // 返回数据
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
