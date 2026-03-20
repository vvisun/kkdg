// Package kkactor is the actor framework for kkdg.
// Package kkactor 提供kkdg的actor框架。
//
// 设计思路：
//  1. 透明化寻址、远程传输。LucencyID（nodeID + actorKey）是 Actor 的全局逻辑标识。
//  2. 对外交互使用 LucencyID；其中 nodeID、actorKey 均须非空且符合 kkapp 命名规则（见 IsValidActorNodeId / IsValidActorKey）。
//     单进程多节点部署时必须填入真实 nodeID，否则无法区分 actor 属于哪个本地节点。
//  3. ActorLocator 根据 nodeID 是否在本进程已登记（AddNode）判定本地/远程；远程传输通过 actorremotes.IRemoteActorTransport
//     注入，可按部署选用 NATS、actorshard（Hub TCP）等实现。
//  4. ActorFramework 内 ActorLocator 与 ActorSystem 必须成对：PID 属于哪个 System 就应由哪个 Framework
//     创建并登记，故 NewActorFramework 不接收外部 Locator 单独注入，避免 locator 与 Spawn 使用的 system 错位。
//
// 目录说明：
//   - actor_id.go：LucencyID 定义与 NewLucencyID 等转换。
//   - framework.go：ActorFramework 是 Actor 框架门面。
//   - actor_locator.go：ActorLocator 是 Actor 寻址系统。
//   - actorremotes：远程协议（信封、MessageRegistry）与 IRemoteActorTransport / IRemoteActorReceiver 抽象。
//   - actornats：基于 NATS 的远程传输实现。
//   - actorshard：基于独立中心服（Hub）TCP 中转的远程传输实现。
//
// 其他进程内远程通道可自行实现 IRemoteActorTransport 并交给 ActorFramework.SetRemoteTransport。
package kkactor
