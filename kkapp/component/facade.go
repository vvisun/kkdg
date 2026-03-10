package component

import (
	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp"
)

// each application is a node. each node is a process.
type IApplication interface {
	kkapp.INodeIdentity
	GetNodeInfo() *kkapp.NodeInfo

	actor.Actor
	GetActorSystem() *actor.ActorSystem
	GetPID() *actor.PID

	Start() error
	Stop() error

	AddComponent(child IComponent) error

	SetConfigDir(configDir string)
	GetConfigDir() string
}

type IComponentLifecycle interface {
	OnInit() error  //初始化组件
	OnStart() error //启动组件
	OnStop() error  //停止组件
}

type IComponent interface {
	GetCompName() string // 组件名称。unique name for the component.
	IComponentLifecycle
	actor.Actor
	GetPID() *actor.PID
	setPID(pid *actor.PID)
	SetApplication(app IApplication)
	GetApplication() IApplication
}
