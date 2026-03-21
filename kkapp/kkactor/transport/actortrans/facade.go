package actortrans

import "time"

// IRemoteActorTransport 抽象跨节点的 actor 消息路由能力。
//
// 典型实现可以基于：
//   - transport/atransnats：NATS pub/sub，按节点订阅 kkactor.send.<nodeId> / kkactor.request.<nodeId>；
//   - transport/atransrelay：经独立 TCP Hub 中继转发；
//   - 其它自定义总线。
//
// 组件和业务代码只依赖该接口，而不关心底层是 NATS、TCP 还是其它实现，可插拔替换。
//
// 运行阶段：
//  transport.SetReceiver(receiver)
//  transport.Start()
//  transport.Send(target, msg)
//  transport.Request(target, msg, timeout)
//  transport.RequestAsync(target, msg, timeout, callback)
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
	// 例如：nats 需要取消订阅；TCP 类实现需关闭连接等。
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
