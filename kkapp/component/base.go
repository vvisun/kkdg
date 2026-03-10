package component

import (
	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/utils/kklog"
)

func IsEqual(a, b kkapp.IComponent) bool {
	if a == nil || b == nil {
		return false
	}
	return a == b || a.GetCompName() == b.GetCompName()
}

type Component struct {
	app kkapp.IApplication
	pid *actor.PID
}

func (slf *Component) SetPID(pid *actor.PID) {
	slf.pid = pid
}

func (slf *Component) GetPID() *actor.PID {
	return slf.pid
}

func (slf *Component) SetApplication(app kkapp.IApplication) {
	slf.app = app
}

func (slf *Component) GetApplication() kkapp.IApplication {
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
