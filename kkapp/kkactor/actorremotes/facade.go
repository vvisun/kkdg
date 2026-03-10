package actorremotes

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
type IRemoteActorTransport interface {
	// Close 关闭底层连接。
	// 例如：nats 需要取消订阅等。 rpc/shard需要关闭连接。
	Close() error
}

// ActorTransportType 是 Actor 传输类型。
type ActorTransportType string

const (
	ActorTransportTypeNats  ActorTransportType = "nats"  // 使用nats集群传输消息
	ActorTransportTypeRpc   ActorTransportType = "rpc"   // 使用rpc传输消息
	ActorTransportTypeShard ActorTransportType = "shard" // 使用shard传输消息
)
