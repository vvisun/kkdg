// Package transport 提供数据传输组件，用于网关与业务服之间的消息转发。
//
// 支持以下传输方式：定义见 defines.go 中的 TransType 常量。
//   - nats：使用nats集群转发消息
//   - rpc：使用rpc转发消息
//   - shard：使用shard转发消息
//
// shard模式的分片数支持：
//   - 8：8个分片。定义见 defines.go 中的 BackendShardCnt 常量。
package transport
