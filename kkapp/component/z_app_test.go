package component

import (
	"testing"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/utils/kklog"
)

type TestComp1 struct {
	Component
}

func (slf *TestComp1) GetCompName() string {
	return "test1"
}

func (slf *TestComp1) OnInit() error {
	kklog.Infof("[kkapp] component %s init", slf.GetCompName())
	return nil
}

func (slf *TestComp1) OnStart() error {
	kklog.Infof("[kkapp] component %s on start", slf.GetCompName())
	return nil
}

func (slf *TestComp1) OnStop() error {
	kklog.Infof("[kkapp] component %s on stop", slf.GetCompName())
	return nil
}

func (slf *TestComp1) Receive(ctx actor.Context) {
	switch ctx.Message().(type) {
	case *actor.Started:
		if err := slf.OnStart(); err != nil {
			kklog.Errorf("[kkapp] component %s on start error: %v", slf.GetCompName(), err)
		}
	case *actor.Stopping:
		if err := slf.OnStop(); err != nil {
			kklog.Errorf("[kkapp] component %s on stop error: %v", slf.GetCompName(), err)
		}
	}
}

type TestComp2 struct {
	Component
}

func (slf *TestComp2) GetCompName() string {
	return "test2"
}

func (slf *TestComp2) OnInit() error {
	kklog.Infof("[kkapp] component %s init", slf.GetCompName())
	return nil
}

func (slf *TestComp2) OnStart() error {
	kklog.Infof("[kkapp] component %s on start", slf.GetCompName())
	return nil
}

func (slf *TestComp2) OnStop() error {
	kklog.Infof("[kkapp] component %s on stop", slf.GetCompName())
	return nil
}

func (slf *TestComp2) Receive(ctx actor.Context) {
	switch ctx.Message().(type) {
	case *actor.Started:
		if err := slf.OnStart(); err != nil {
			kklog.Errorf("[kkapp] component %s on start error: %v", slf.GetCompName(), err)
		}
	case *actor.Stopping:
		if err := slf.OnStop(); err != nil {
			kklog.Errorf("[kkapp] component %s on stop error: %v", slf.GetCompName(), err)
		}
	}
}

//-------------------------------- test application --------------------------------

func TestApplication_AddComponent(t *testing.T) {
	app := NewApplication(kkapp.NewNodeInfo("node1", "test", "127.0.0.1:8080", "", nil))
	// 测试正常添加
	if err := app.AddComponent(&TestComp1{}); err != nil {
		t.Fatalf("add component: %v", err)
	}
	// 测试重复添加，应该返回错误
	if err := app.AddComponent(&TestComp1{}); err == nil {
		t.Fatalf("add component: %v should return error", err)
	}
	if err := app.AddComponent(&TestComp2{}); err != nil {
		t.Fatalf("add component: %v", err)
	}

	// 测试启动
	if err := app.Start(); err != nil {
		t.Fatalf("start application: %v", err)
	}

	// 给 actor 时间处理 Started 和子组件 OnStart，便于看到日志
	time.Sleep(50 * time.Millisecond)

	// 测试停止
	if err := app.Stop(); err != nil {
		t.Fatalf("stop application: %v", err)
	}
}
