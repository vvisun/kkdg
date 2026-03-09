// Package ccgate 提供网关服组件。
//
// 网关职责：接收客户端连接；数据转发（客户端 <-> 网关 <-> 业务服）。
// 传输层转发方式可选：
//   - "nats"（由NATS集群转发）
//   - "rpc"（rpc转发）
//   - "shard"（由shard连接池转发）
package ccgate
