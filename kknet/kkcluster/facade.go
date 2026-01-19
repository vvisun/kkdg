package kkcluster

import "time"

type (
	// IDiscovery 发现服务接口
	IDiscovery interface {
		Name() string                                                 // 发现服务名称
		Map() map[string]IMember                                      // 获取成员列表
		ListByType(nodeType string, filterNodeID ...string) []IMember // 根据节点类型获取列表
		Random(nodeType string) (IMember, bool)                       // 根据节点类型随机一个
		GetType(nodeID string) (nodeType string, err error)           // 根据节点id获取类型
		GetMember(nodeID string) (member IMember, found bool)         // 获取成员
		AddMember(member IMember)                                     // 添加成员
		RemoveMember(nodeID string)                                   // 移除成员
		OnAddMember(listener MemberListener)                          // 添加成员监听函数
		OnRemoveMember(listener MemberListener)                       // 移除成员监听函数
		Stop()
	}

	// IMember 成员接口
	IMember interface {
		GetNodeID() string
		GetNodeType() string
		GetAddress() string
		GetSettings() map[string]string
	}

	// MemberListener 成员增、删监听函数
	MemberListener func(member IMember)
)

type (
	// ICluster 集群接口
	ICluster interface {
		Init()                                                                                        // 初始化
		PublishRemote(nodeID string, packet *ClusterPacket) error                                     // 发布消息
		PublishRemoteType(nodeType string, packet *ClusterPacket) error                               // 根据节点类型发布消息
		RequestRemote(nodeID string, packet *ClusterPacket, timeout ...time.Duration) ([]byte, int32) // 请求消息
		Stop()                                                                                        // 停止
	}
)

// ClusterPacket 集群消息包
type ClusterPacket struct {
	BuildTime  int64    `json:"buildTime,omitempty"`
	SourcePath string   `json:"sourcePath,omitempty"`
	TargetPath string   `json:"targetPath,omitempty"`
	FuncName   string   `json:"funcName,omitempty"`
	ArgBytes   []byte   `json:"argBytes,omitempty"`
	Session    *Session `json:"session,omitempty"`
}

// Session 会话
type Session struct {
	Sid       string            `json:"sid,omitempty"`       // 会话唯一id
	Uid       int64             `json:"uid,omitempty"`       // 用户id
	AgentPath string            `json:"agentPath,omitempty"` // 前端actor agent路径
	Ip        string            `json:"ip,omitempty"`        // ip地址
	Data      map[string]string `json:"data,omitempty"`      // 扩展数据
}

var (
	defaultNatsAddress = "nats://127.0.0.1:4222"
)

// NewNatsDiscoveryWithDefaults 使用默认配置创建NATS服务发现
func NewNatsDiscoveryWithDefaults(name, nodeID, nodeType, address string) *NatsDiscovery {
	return NewNatsDiscovery(name, nodeID, nodeType, address, defaultNatsAddress, nil)
}

// NewNatsClusterWithDefaults 使用默认配置创建NATS集群
func NewNatsClusterWithDefaults(nodeID string, discovery IDiscovery) *NatsCluster {
	return NewNatsCluster(nodeID, discovery, defaultNatsAddress)
}
