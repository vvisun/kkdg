package kkcluster

import (
	"sync/atomic"

	"github.com/vvisun/kkdg/utils/kkpool"
)

type (
	// ClusterPacket 集群消息包
	ClusterPacket struct {
		BuildTime  int64  `json:"buildTime,omitempty"`  // 构建时间（毫秒）
		Timeout    int64  `json:"timeout,omitempty"`    // 函数请求超时时间(毫秒)
		SourcePath string `json:"sourcePath,omitempty"` // 源节点路径
		TargetPath string `json:"targetPath,omitempty"` // 目标节点路径
		FuncName   string `json:"funcName,omitempty"`   // 函数名
		ArgBytes   []byte `json:"argBytes,omitempty"`   // 函数参数

		Sid        string            `json:"sid,omitempty"`        // 会话唯一id
		Uid        int64             `json:"uid,omitempty"`        // 用户id
		AgentPath  string            `json:"agentPath,omitempty"`  // 前端actor agent路径
		Ip         string            `json:"ip,omitempty"`         // ip地址
		ExtendData map[string]string `json:"extendData,omitempty"` // 扩展数据(可选项，可以为空)
	}
)

type (
	// ClusterRequest 集群请求消息
	ClusterRequest struct {
		RequestID    string         `json:"requestID"`    // 请求id（自动生成）
		SourceNodeID string         `json:"sourceNodeID"` // 源节点id
		Packet       *ClusterPacket `json:"packet"`       // 消息包
	}

	// ClusterResponse 集群响应消息
	ClusterResponse struct {
		RequestID string `json:"requestID"` // 请求id（与请求消息的requestID相同）
		Code      int32  `json:"code"`      // 错误码（ClusterErrorCode），0表示成功，其他表示失败
		Data      []byte `json:"data"`      // 返回数据
	}
)

const max_size_for_pool = 4096

var gPoolSize atomic.Int64 //池中对象数量
var gClusterPacketPool = kkpool.NewSfxPool(func() *ClusterPacket { return &ClusterPacket{} })

func NewClusterPacket() *ClusterPacket {
	if gPoolSize.Load() > 0 {
		gPoolSize.Add(-1)
	}
	return gClusterPacketPool.Get()
}

var emptyClusterPacket = ClusterPacket{}

func PutClusterPacket(packet *ClusterPacket) {
	if packet == nil {
		return
	}
	if gPoolSize.Load() > max_size_for_pool {
		return // 直接丢弃，防止池过大耗尽内存
	}
	*packet = emptyClusterPacket // 清空数据
	gClusterPacketPool.Put(packet)
}
