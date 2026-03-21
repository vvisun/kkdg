// Package actorhub 提供 Actor 注册与查询（Hub 协议与可替换实现）。
//
// 典型流程：节点在 Hub 上登记 LucencyID（见 hubproto.RegisterActorReq，含 NodeInfo / RpcAddress 等），
// 查询结果可缓存在 RemoteActorMgr（由 Hub 客户端或依赖目录的传输侧持有，不挂在 kkactor.ActorFramework）。
// 业务消息仍通过 actortrans.IRemoteActorTransport 发送。
package actorhub
