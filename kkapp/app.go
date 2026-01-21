package kkapp

import (
	"github.com/vvisun/kkdg/kkapp/component"
	"github.com/vvisun/kkdg/utils/kklog"
)

type Application struct {
	component.Component
}

var _ component.IComponent = (*Application)(nil)

func NewApplication() *Application {
	return &Application{}
}

func (slf *Application) GetID() string {
	return "application"
}

func (slf *Application) Start() error {
	for _, child := range slf.GetChildrens() {
		if err := child.Start(); err != nil {
			kklog.Errorf("[kkapp] application %s start child %s error: %v", slf.GetName(), child.GetName(), err)
			return err
		}
		kklog.Infof("[kkapp] application %s start child %s success", slf.GetName(), child.GetName())
	}
	return nil
}

func (slf *Application) Stop() error {
	childlist := slf.GetChildrens()
	for i := len(childlist) - 1; i >= 0; i-- {
		if err := childlist[i].Stop(); err != nil {
			kklog.Errorf("[kkapp] application %s stop child %s error: %v", slf.GetName(), childlist[i].GetName(), err)
		}
		kklog.Infof("[kkapp] application %s stop child %s success", slf.GetName(), childlist[i].GetName())
	}
	return nil
}
