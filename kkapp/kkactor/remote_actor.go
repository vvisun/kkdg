package kkactor

import (
	"github.com/vvisun/kkdg/kkapp"
)

// IRemoteActorTransport 抽象跨节点的 actor 消息路由能力。
//
// 典型实现可以基于：
//   - kkcluster（NATS）：使用 subject = "actor.<nodeId>.<actorKey>" 做 pub/sub；
//   - kkrpc：注册 Forward 接口，在 RPC handler 中将 payload 投递到本地 ActorSystem；
//   - 其它自定义总线。
//
// 组件和业务代码只依赖该接口，而不关心底层是 NATS、RPC 还是其它实现，从而做到类似 TransType(nats/rpc/shard)
// 那样可插拔替换。
type IRemoteActorTransport interface {
	// Init 在 transport 挂接到 Application 时被调用，传入本节点的 NodeInfo。
	// 典型实现可以在此基于 nodeInfo 初始化订阅（如 NATS subject）、注册 RPC 服务等。
	Init(self *kkapp.NodeInfo) error

	// SetReceiver 注册本节点用于接收远程 actor 消息的回调。
	// 实现需要在收到来自网络/总线的消息时调用该回调，将消息交给上层（通常由组件负责路由到本地 PID）。
	SetReceiver(fn func(from AlphaActorID, msg any))

	// TellRemote 将消息发送到目标节点上的某个 actor。
	// 具体序列化格式可复用 kkapp.GetTransMsgPacket 或自定义。
	TellRemote(target AlphaActorID, msg any) error

	// Close 关闭底层连接、取消订阅等资源。
	Close() error
}
