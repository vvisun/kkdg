// Package kkactor is the actor framework for kkdg.
// Package kkactor 提供kkdg的actor框架。
//
// 设计思路：
//  1. 透明化寻址、远程传输。LucencyActorID 是 Actor 的唯一标识。
//  2. actor之间的交互只关心actorID，不需要关心actor所在节点。
//  3. actor远程交互的传输层可以基于nats/rpc/shard等实现。外部无需关心底层实现，可以任意替换。
//
// 目录说明：
//   - actor_id.go：LucencyActorID 是 Actor 的唯一标识。
//   - framework.go：ActorFramework 是 Actor 框架门面。
//   - locator.go：ActorLocator 是 Actor 寻址系统。
//   - actorremotes：actor远程交互的传输层公共部分，包含协议，和传输抽象接口定义。
//   - actornats：基于nats的actor远程交互的传输层。
//   - actorrpc：基于rpc的actor远程交互的传输层。
//   - actorshard：基于独立中心服（Hub）TCP 中转的 actor 远程传输层。
package kkactor
