package kkapp

import (
	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp/faultreport"
	"github.com/vvisun/kkdg/kkapp/kkactor"
	"github.com/vvisun/kkdg/utils/kkevent"
)

// INodeIdentity 节点身份接口。
type INodeIdentity interface {
	GetNodeId() string   // 获取节点ID。世界唯一。用于标识一个节点。
	GetNodeType() string // 获取节点类型。eg: gate、game、login等。用于标识一个节点的类型。
}

// each application is a node.
type IApplication interface {
	INodeIdentity
	GetNodeInfo() *NodeInfo

	actor.Actor
	GetPID() *actor.PID
	GetCompPID(compName string) *actor.PID

	Start() error
	Stop() error

	AddComponent(child IComponent) error

	GetConfigDir() string
	GetOptions() *AppOptions
	GetActorFramework() *kkactor.ActorFramework

	GetFaultEventMgr() *kkevent.SpecEventManager[string, *faultreport.ComponentFaultEvent]

	// 设置扩展数据，用于存储一些自定义数据
	SetExtData(data any)
	// 获取扩展数据
	GetExtData() any
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
	SetPID(pid *actor.PID)
	SetApplication(app IApplication)
	GetApplication() IApplication
}
