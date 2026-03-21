// Package kkmodule 提供基于树形结构的模块组织与生命周期（OnInit / OnStop），不依赖 ProtoActor。
//
// 与 kkapp（Application + Component + kkactor）是并列的另一种应用层组织方式：
//   - kkapp：节点即 Actor，组件即子 Actor，跨节点通信走 ActorFramework。
//   - kkmodule：按模块 ID / 父子关系组装的模块树，适合不需要 Actor 消息模型的场景。
//
// 二者可同时存在于同一仓库的不同进程或服务中，但通常不在同一套业务里混用两套根架构。
package kkmodule
