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
