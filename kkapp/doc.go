// Package kkapp 提供应用程序框架。该包主要用于展示如何使用kkdg框架。
//
// 注意：在app初始化阶段，调用envconfig.ConfigDefaults()配置默认值。运行期间不要修改。
//
// 架构说明:
//  1. 应用Application = 节点 = 根Actor = 组件容器
//  2. 组件Component = Application的子Actor
//  3. 节点之间通过ActorFramework通信
//  4. 组件Component之间通过ActorFramework通信
//
// 一、由于每个节点都是一个Actor, 所以Application可以像其他Actor一样被寻址，从而实现透明化。
// 这样我们可以做到，单机部署和集群部署都无需修改逻辑。
//
// 二、由于每个组件视为1个actor，所以我们可以做到：
//   - 组件挂接到任意节点上时，都能实现透明化。逻辑层只关心“给目标节点发消息”，不需要知道对端到底在不在本进程
//   - 单机部署，集群部署都无需修改逻辑。
//   - 调整组件所属节点时，也无需修改逻辑。
//   - 例如：
//     comp1, comp2, comp3. 分别挂接在node1, node2, node3上, node1部署在机器1，node2部署在机器2，node3部署在机器3。
//     和 comp1,comp2 挂接在node1, comp3 挂接在node2, node1部署在机器1，node2部署在机器2，效果是一样的。可以任意组合。
//
// 三、网关与业务服之间通过transportor进行消息转发，transportor支持以下传输方式：
//   - nats：使用nats集群转发消息
//   - rpc：使用rpc转发消息
//   - shard：使用shard转发消息
//
// shard模式的分片数支持：
//   - 8：8个分片。宏定义，经大量测试实践，8个分片时性能最优。
//
// shard和rpc模式时，不依赖服务发现和集群，只需要逻辑服连接并注册到网关即可，扩容方便，部署方便。
// 大规模部署时，可以考虑加入服务发现和集群。
package kkapp
