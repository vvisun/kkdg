package actorremotes

import "time"

// IRemoteActorTransport 抽象跨节点的 actor 消息路由能力。
//
// 典型实现可以基于：
//   - kkcluster（NATS）：使用 subject = "actor.<nodeId>.<actorKey>" 做 pub/sub；
//   - kkrpc：注册 Forward 接口，在 RPC handler 中将 payload 投递到本地 ActorSystem；
//   - kknet：类似网关的shard分流切片
//   - 其它自定义总线。
//
// 组件和业务代码只依赖该接口，而不关心底层是 NATS、RPC 还是其它实现，从而做到类似 TransType(nats/rpc/shard)
// 那样可插拔替换。
type IRemoteActorReceiver interface {
	// HandleRemoteSend 处理来自远程节点的单向消息。
	HandleRemoteSend(target ActorRef, msg any) error
	// HandleRemoteRequest 处理来自远程节点的请求消息。
	HandleRemoteRequest(target ActorRef, msg any, timeout time.Duration) (any, error)
}

type IRemoteActorTransport interface {
	// Start 启动底层连接与订阅。
	Start() error
	// Close 关闭底层连接。
	// 例如：nats 需要取消订阅等。 rpc/shard需要关闭连接。
	Close() error
	// SetReceiver 设置本地接收器，用于把远程消息投递到当前进程。
	SetReceiver(receiver IRemoteActorReceiver)
	// Send 向远程 actor 发送单向消息。
	Send(target ActorRef, msg any) error
	// Request 向远程 actor 发送请求并等待响应。
	Request(target ActorRef, msg any, timeout time.Duration) (any, error)
	// RequestAsync 向远程 actor 发送异步请求。
	RequestAsync(target ActorRef, msg any, timeout time.Duration, callback func(result any, err error)) error
}

// ActorTransportType 是 Actor 传输类型。
type ActorTransportType string

const (
	ActorTransportTypeNats  ActorTransportType = "nats"  // 使用nats集群传输消息
	ActorTransportTypeRpc   ActorTransportType = "rpc"   // 使用rpc传输消息
	ActorTransportTypeShard ActorTransportType = "shard" // 使用shard传输消息
)
