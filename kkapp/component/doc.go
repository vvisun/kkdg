// Package component: application core package.
// 本包定义了应用程序的核心组件接口和实现。
//
// Application: Node + ActorSystem + Actor
//
//  1. 是1个节点，也是一个ActorSystem, 同时也是一个Actor。
//
//  2. Application是组件的容器，组件是Application的子Actor。
//
//     由于每个节点都是一个Actor, 所以Application可以像其他Actor一样被寻址，从而实现透明化。
//     这样我们可以做到，单机部署和集群部署都无需修改逻辑。
//
// 每个组件视为1个actor。这样，我们可以做到：
//   - 组件挂接到任意节点上时，都能实现透明化。逻辑层只关心“给目标节点发消息”，不需要知道对端到底在不在本进程
//   - 单机部署，集群部署都无需修改逻辑。
//   - 调整组件所属节点时，也无需修改逻辑。
//   - 例如：
//     comp1, comp2, comp3. 分别挂接在node1, node2, node3上, node1部署在机器1，node2部署在机器2，node3部署在机器3。
//     和 comp1,comp2 挂接在node1, comp3 挂接在node2, node1部署在机器1，node2部署在机器2，效果是一样的。可以任意组合。
package component
