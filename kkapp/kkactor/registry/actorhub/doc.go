// Package actorhub 提供 Actor 注册与查询（Hub 协议与可替换实现）。
//
// 典型流程：节点在 Hub 上登记 LucencyID（见 hubproto.RegisterActorReq，含 NodeInfo / RpcAddress 等），
// 查询得到目标节点信息后，再通过 actortrans.IRemoteActorTransport 发送业务消息。
package actorhub
