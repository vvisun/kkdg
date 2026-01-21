package kkapp

import (
	"github.com/vvisun/kkdg/kkapp/component"
	"github.com/vvisun/kkdg/utils/kklog"
)

type IApplication interface {
	Start() error
	Stop() error
}

type Application struct {
	component.Component
}

var _ component.IComponent = (*Application)(nil)
var _ IApplication = (*Application)(nil)

func NewApplication() *Application {
	return &Application{}
}

func (slf *Application) GetID() string {
	return "application"
}

func (slf *Application) Start() error {
	return nil
}

func (slf *Application) Stop() error {
	if err := slf.BeforeShutdown(); err != nil {
		kklog.Errorf("[kkapp] application %s before shutdown error: %v", slf.GetName(), err)
	}
	err := slf.Component.Stop()
	if err != nil {
		kklog.Errorf("[kkapp] application %s stop error: %v", slf.GetName(), err)
	}
	return nil
}
