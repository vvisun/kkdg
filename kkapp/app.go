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
	if err := slf.Init(); err != nil {
		kklog.Errorf("[kkapp] application start failed: %v", err)
		return err
	}
	return nil
}

func (slf *Application) Stop() error {
	if err := slf.BeforeShutdown(); err != nil {
		kklog.Errorf("[kkapp] application before shutdown failed: %v", err)
		return err
	}
	if err := slf.Shutdown(); err != nil {
		kklog.Errorf("[kkapp] application shutdown failed: %v", err)
		return err
	}
	return nil
}
