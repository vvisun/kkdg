// Package transport 提供数据传输组件。
package transport

// 传输层类型。用于网关与业务服之间的消息转发。
type TransType = string

const (
	TransTypeNats  TransType = "nats"  // 使用nats集群转发消息
	TransTypeRpc   TransType = "rpc"   // 使用rpc转发消息
	TransTypeShard TransType = "shard" // 使用shard转发消息
)

const BackendShardCnt = 8
