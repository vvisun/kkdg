// Package kkactor is the actor framework for kkdg.
// Package kkactor 提供kkdg的actor框架。
//
// 设计思路：
//  1. 透明化寻址、远程传输。LucencyID（nodeID + actorKey）是 Actor 的全局逻辑标识。
//  2. 对外交互使用 LucencyID；
//     其中 nodeID、actorKey 均须非空且符合 kkapp 命名规则（见 IsValidActorNodeId / IsValidActorKey）。
//     单进程多节点部署时必须填入真实 nodeID，否则无法区分 actor 属于哪个本地节点。
//  3. ActorLocator：仅维护本地 nodeID 集合与 PID 表；nodeID 在集合中即本地路由。AddActor 会自动把对应 nodeID 加入集合。
//     完整 *NodeInfo 不由 Locator 持有，可另设本地 NodeInfo 管理器。
//     远程目录（如 RemoteActorMgr）由注册/传输侧持有；远程投递通过 actortrans.IRemoteActorTransport 注入。
//  4. ActorFramework 内 ActorLocator 与 ActorSystem 必须成对：PID 属于哪个 System 就应由哪个 Framework
//     创建并登记，故 NewActorFramework 不接收外部 Locator 单独注入，避免 locator 与 Spawn 使用的 system 错位。
//
// 目录说明：
//   - actor_id.go：LucencyID 定义与 NewLucencyID 等转换。
//   - framework.go：ActorFramework 是 Actor 框架门面。
//   - actor_locator.go：ActorLocator 是本进程本地 Actor（PID）登记与查询。
//   - transport: actor远程传输实现。
//     其他进程内远程通道可自行实现 actortrans.IRemoteActorTransport 并交给 ActorFramework.SetRemoteTransport。
//     1. transport/actortrans：IRemoteActorTransport / IRemoteActorReceiver 抽象。
//     2. transport/atransnats：基于 NATS 的远程传输实现。
//     3. transport/atransrelay：基于独立 TCP 中心服（Hub）中继转发的远程传输实现。
//   - registry: actor注册中心。用于Actor的全网寻址，自动发现与注册。
//     其他注册中心可自行实现。
//     1. registry/hubproto：actor注册中心协议。
//     2. registry/actorhub：actor注册中心接口。
//     3. registry/hubtcp：基于独立中心服（Hub）TCP 的actor注册中心实现。
package kkactor
