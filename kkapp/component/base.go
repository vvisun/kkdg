package component

import (
	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/xreflect"
)

func IsEqual(a, b IComponent) bool {
	if a == nil || b == nil {
		return false
	}
	return a == b || a.GetCompName() == b.GetCompName()
}

func GetComponentName(comp IComponent) string {
	return xreflect.GetStructName(comp) + "_" + comp.GetCompName()
}

type ComponentState = int64

const (
	ComponentStateNone     ComponentState = iota //组件未初始化
	ComponentStateStarting                       //组件启动中
	ComponentStateStarted                        //组件已启动
	ComponentStateStoping                        //组件停止中
	ComponentStateStoped                         //组件已停止
)

var stateNameMap = map[ComponentState]string{
	ComponentStateNone:     "none",
	ComponentStateStarting: "starting",
	ComponentStateStarted:  "started",
	ComponentStateStoping:  "stoping",
	ComponentStateStoped:   "stoped",
}

func GetStateName(state ComponentState) string {
	return stateNameMap[state]
}

type Component struct {
	app  IApplication
	pid  *actor.PID
	self *Component
}

func (slf *Component) setPID(pid *actor.PID) {
	slf.pid = pid
}

func (slf *Component) GetPID() *actor.PID {
	return slf.pid
}

func (slf *Component) SetApplication(app IApplication) {
	slf.app = app
}

func (slf *Component) GetApplication() IApplication {
	return slf.app
}

func (slf *Component) OnInit() error {
	kklog.Debug("[kkapp] component 子类未实现OnInit方法")
	return nil
}

func (slf *Component) OnStart() error {
	kklog.Debug("[kkapp] component 子类未实现OnStart方法")
	return nil
}

func (slf *Component) OnStop() error {
	kklog.Debug("[kkapp] component 子类未实现OnStop方法")
	return nil
}
