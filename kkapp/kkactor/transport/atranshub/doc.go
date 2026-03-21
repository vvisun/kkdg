// Package actorshard 通过独立 TCP 中心服（Hub）注册节点并中转 Actor 远程消息，不依赖 kkapp/transport 网关或 NATS。
//
// 服务端：NewHub(addr, stream).Start()
// 客户端节点：NewTransport(nodeId, registry, Options{HubAddr: "host:port"}).Start()
//
// 帧格式与 kktcp 默认流一致：[length][wireType+msgpack(body)]。
package atranshub
