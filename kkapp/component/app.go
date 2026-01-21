package component

import (
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kklog"
)

type IApplication interface {
	GetID() string
	Start() error
	Stop() error
	AddCompenent(child IComponent) error
	HasComponent(child IComponent) bool
	GetComponents() []IComponent
}

type Application struct {
	compList []IComponent
}

func NewApplication() *Application {
	return &Application{}
}

func (slf *Application) GetID() string {
	return "application"
}

func (slf *Application) Start() error {
	childlist := slf.compList
	for _, child := range childlist {
		if err := child.Start(); err != nil {
			kklog.Errorf("[kkapp] application %s start child %s error: %v", slf.GetID(), child.GetID(), err)
			return err
		}
		kklog.Infof("[kkapp] application %s start child %s success", slf.GetID(), child.GetID())
	}
	return nil
}

func (slf *Application) Stop() error {
	childlist := slf.compList
	for i := len(childlist) - 1; i >= 0; i-- {
		if err := childlist[i].Stop(); err != nil {
			kklog.Errorf("[kkapp] application %s stop child %s error: %v", slf.GetID(), childlist[i].GetID(), err)
		}
		kklog.Infof("[kkapp] application %s stop child %s success", slf.GetID(), childlist[i].GetID())
	}
	return nil
}

func (slf *Application) AddCompenent(child IComponent) error {
	if slf.HasComponent(child) {
		return kkerrors.ErrComponentAlreadyAdded
	}
	child.SetApplication(slf)
	slf.compList = append(slf.compList, child)
	return nil
}

func (slf *Application) HasComponent(child IComponent) bool {
	for _, c := range slf.compList {
		if IsEqual(c, child) {
			return true
		}
	}
	return false
}

func (slf *Application) GetComponents() []IComponent {
	return slf.compList
}
