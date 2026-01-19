package kkcluster

type (
	// ClusterPacket 集群消息包
	ClusterPacket struct {
		BuildTime  int64    `json:"buildTime,omitempty"`
		SourcePath string   `json:"sourcePath,omitempty"`
		TargetPath string   `json:"targetPath,omitempty"`
		FuncName   string   `json:"funcName,omitempty"`
		ArgBytes   []byte   `json:"argBytes,omitempty"`
		Session    *Session `json:"session,omitempty"`
	}
	// Session 会话
	Session struct {
		Sid       string            `json:"sid,omitempty"`       // 会话唯一id
		Uid       int64             `json:"uid,omitempty"`       // 用户id
		AgentPath string            `json:"agentPath,omitempty"` // 前端actor agent路径
		Ip        string            `json:"ip,omitempty"`        // ip地址
		Data      map[string]string `json:"data,omitempty"`      // 扩展数据
	}
)

type (
	// ClusterRequest 集群请求消息
	ClusterRequest struct {
		RequestID    string         `json:"requestID"`
		SourceNodeID string         `json:"sourceNodeID"`
		Packet       *ClusterPacket `json:"packet"`
	}

	// ClusterResponse 集群响应消息
	ClusterResponse struct {
		RequestID string `json:"requestID"`
		Code      int32  `json:"code"`
		Data      []byte `json:"data"`
	}
)
