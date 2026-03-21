// Package atransrelay 通过独立 TCP 中心服（Hub）注册节点并中转 Actor 远程消息。
//
// 服务端：NewHub(addr).Start()
// 客户端节点：NewTransport(nodeId, registry, Options{HubAddr: "host:port"}).Start()
package atransrelay
