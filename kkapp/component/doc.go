// Package component: application core package.
// 本包定义了应用程序的核心组件接口和实现。
//
// 每个应用程序视为1个节点。每个节点视为1个进程。
// 可以在同一个应用里启动多个节点，视为单机部署。一个应用启动1个节点，视为集群部署。
// 每个节点可以包含多个组件。
//
// 每个组件视为1个actor。这样，我们可以做到：
//   - 组件挂接到任意节点上时，都能实现透明化。
//   - 单机部署，集群部署都无需修改逻辑。
//   - 调整组件所属节点时，也无需修改逻辑。
package component

import (
	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp"
)

// each application is a node. each node is a process.
type IApplication interface {
	kkapp.INodeIdentity
	GetNodeInfo() *kkapp.NodeInfo
	Start() error
	Stop() error
	GetActorSystem() *actor.ActorSystem

	AddComponent(child IComponent) error
	HasComponent(child IComponent) bool
	GetComponents() []IComponent

	SetConfigDir(configDir string)
	GetConfigDir() string
}

type IComponentLifecycle interface {
	Init() error  //初始化组件
	Start() error //启动组件
	Stop() error  //停止组件
}

type IComponent interface {
	GetCompName() string // 组件名称。unique name for the component.
	IComponentLifecycle
	SetApplication(app IApplication)
	GetApplication() IApplication
	Equal(other IComponent) bool
}
