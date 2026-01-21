package kkcluster

type (
	// MemberInfo 成员信息（用于序列化）
	MemberInfo struct {
		NodeID   string            `json:"nodeID"`
		NodeType string            `json:"nodeType"`
		Address  string            `json:"address"`
		Settings map[string]string `json:"settings"`
	}

	// DiscoveryRequest 发现请求
	DiscoveryRequest struct {
		RequesterID string `json:"requesterID"`
	}
)

type (
	// ClusterPacket 集群消息包
	ClusterPacket struct {
		BuildTime  int64    `json:"buildTime,omitempty"`  // 构建时间（毫秒）
		SourcePath string   `json:"sourcePath,omitempty"` // 源节点路径
		TargetPath string   `json:"targetPath,omitempty"` // 目标节点路径
		FuncName   string   `json:"funcName,omitempty"`   // 函数名
		ArgBytes   []byte   `json:"argBytes,omitempty"`   // 函数参数
		Timeout    int64    `json:"timeout,omitempty"`    // 函数请求超时时间(毫秒)
		Session    *Session `json:"session,omitempty"`    // 会话(可选项，可以为空)
	}
	// Session 会话
	Session struct {
		Sid       string            `json:"sid,omitempty"`       // 会话唯一id
		Uid       int64             `json:"uid,omitempty"`       // 用户id
		AgentPath string            `json:"agentPath,omitempty"` // 前端actor agent路径
		Ip        string            `json:"ip,omitempty"`        // ip地址
		Data      map[string]string `json:"data,omitempty"`      // 扩展数据(可选项，可以为空)
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
