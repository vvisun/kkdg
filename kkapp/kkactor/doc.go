// Package kkactor is the actor framework for kkdg.
// Package kkactor 提供kkdg的actor框架。
//
// 设计思路：
//  1. 透明化寻址、远程传输。LucencyActorID 是 Actor 的唯一标识。
//  2. actor之间的交互只关心 actorID，不需要关心 actor 所在节点。
//  3. 远程传输通过 actorremotes.IRemoteActorTransport 注入，可按部署选用 NATS、ActorHub（TCP）等实现。
//
// 目录说明：
//   - actor_id.go：LucencyActorID 是 Actor 的唯一标识。
//   - framework.go：ActorFramework 是 Actor 框架门面。
//   - actor_locator.go：ActorLocator 是 Actor 寻址系统。
//   - actorremotes：远程协议（信封、MessageRegistry）与 IRemoteActorTransport / IRemoteActorReceiver 抽象。
//   - actornats：基于 NATS 的远程传输实现。
//   - actorshard：基于独立中心服（Hub）TCP 中转的远程传输实现。
//
// 其他进程内远程通道可自行实现 IRemoteActorTransport 并交给 ActorFramework.SetRemoteTransport。
package kkactor
