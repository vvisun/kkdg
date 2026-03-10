package component

import (
	"fmt"
	"testing"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/xreflect"
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
	kklog.Debugf("--------- component %s receive message: %v", slf.GetCompName(), xreflect.GetStructName(ctx.Message()))
	switch ctx.Message().(type) {
	case *actor.Stopping:
		if err := slf.OnStop(); err != nil {
			kklog.Errorf("[kkapp] component %s on stop error: %v", slf.GetCompName(), err)
		}
	}
}

// 性能基准用的轻量组件（不打印日志，减少干扰）
type benchComp struct {
	Component
	name string
}

func (slf *benchComp) GetCompName() string {
	if slf.name == "" {
		slf.name = "bench"
	}
	return slf.name
}

func (slf *benchComp) OnInit() error  { return nil }
func (slf *benchComp) OnStart() error { return nil }
func (slf *benchComp) OnStop() error  { return nil }

func (slf *benchComp) Receive(ctx actor.Context) {
	switch ctx.Message().(type) {
	case *actor.Stopping:
		_ = slf.OnStop()
	}
}

type failStartComp struct {
	Component
}

func (slf *failStartComp) GetCompName() string { return "fail_start" }
func (slf *failStartComp) OnInit() error       { return nil }
func (slf *failStartComp) OnStart() error      { return fmt.Errorf("start failed") }
func (slf *failStartComp) OnStop() error       { return nil }
func (slf *failStartComp) Receive(ctx actor.Context) {
	switch ctx.Message().(type) {
	case *actor.Stopping:
		_ = slf.OnStop()
	}
}

//-------------------------------- test application --------------------------------

func TestApplication_AddComponent(t *testing.T) {
	app := NewApplication(kkapp.NewNodeInfo("node1", "test", "127.0.0.1:8080", "", nil), nil)
	// 测试正常添加
	if err := app.AddComponent(&TestComp1{}); err != nil {
		t.Fatalf("add component: %v", err)
	}
	// 测试重复添加，应该返回错误
	// if err := app.AddComponent(&TestComp1{}); err == nil {
	// 	t.Fatalf("add component: %v should return error", err)
	// }
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

// 测试重复启动应用
func TestApplication_StartTwice(t *testing.T) {
	app := NewApplication(kkapp.NewNodeInfo("node1", "test", "127.0.0.1:8080", "", nil), nil)

	if err := app.Start(); err != nil {
		t.Fatalf("first start application: %v", err)
	}

	// 第二次启动应该返回 ErrAppAlreadyStarted
	if err := app.Start(); err != kkerrors.ErrAppAlreadyStarted {
		t.Fatalf("second start application should return ErrAppAlreadyStarted, got: %v", err)
	}

	// 清理
	time.Sleep(10 * time.Millisecond)
	if err := app.Stop(); err != nil {
		t.Fatalf("stop application: %v", err)
	}
}

// 测试未启动时停止应用
func TestApplication_Stop_NotStarted(t *testing.T) {
	app := NewApplication(kkapp.NewNodeInfo("node1", "test", "127.0.0.1:8080", "", nil), nil)

	if err := app.Stop(); err != kkerrors.ErrAppNotStarted {
		t.Fatalf("stop application when not started should return ErrAppNotStarted, got: %v", err)
	}
}

// 测试非 None 状态下添加组件
func TestApplication_AddComponent_NotNoneState(t *testing.T) {
	app := NewApplication(kkapp.NewNodeInfo("node1", "test", "127.0.0.1:8080", "", nil), nil)

	// 先添加一个组件
	if err := app.AddComponent(&TestComp1{}); err != nil {
		t.Fatalf("add component: %v", err)
	}

	// 启动应用，使 state 从 None 变为 Starting/Started
	if err := app.Start(); err != nil {
		t.Fatalf("start application: %v", err)
	}

	// 启动后再添加组件应该失败
	if err := app.AddComponent(&TestComp2{}); err != kkerrors.ErrAppAddCompMustInNoneState {
		t.Fatalf("add component after start should return ErrAppAddCompMustInNoneState, got: %v", err)
	}

	// 清理
	time.Sleep(10 * time.Millisecond)
	if err := app.Stop(); err != nil {
		t.Fatalf("stop application: %v", err)
	}
}

// 测试获取子组件 PID
func TestApplication_GetChildPID(t *testing.T) {
	app := NewApplication(kkapp.NewNodeInfo("node1", "test", "127.0.0.1:8080", "", nil), nil)

	if err := app.AddComponent(&TestComp1{}); err != nil {
		t.Fatalf("add component: %v", err)
	}

	if err := app.Start(); err != nil {
		t.Fatalf("start application: %v", err)
	}

	// 给 actor 时间处理 Started 和子组件 OnStart
	time.Sleep(50 * time.Millisecond)

	pid := app.GetCompPID("test1")
	if pid == nil {
		t.Fatalf("GetChildPID(\"test1\") should not be nil")
	}

	if unknown := app.GetCompPID("not-exists"); unknown != nil {
		t.Fatalf("GetChildPID(\"not-exists\") should be nil, got: %v", unknown)
	}

	if err := app.Stop(); err != nil {
		t.Fatalf("stop application: %v", err)
	}
}

func TestApplication_Start_WaitsForComponentStart(t *testing.T) {
	app := NewApplication(kkapp.NewNodeInfo("node1", "test", "127.0.0.1:8080", "", nil), nil)

	if err := app.AddComponent(&TestComp1{}); err != nil {
		t.Fatalf("add component: %v", err)
	}

	if err := app.Start(); err != nil {
		t.Fatalf("start application: %v", err)
	}

	if pid := app.GetCompPID("test1"); pid == nil {
		t.Fatal("component pid should be available immediately after Start returns")
	}

	if err := app.Stop(); err != nil {
		t.Fatalf("stop application: %v", err)
	}
}

func TestApplication_Start_ComponentStartError(t *testing.T) {
	app := NewApplication(kkapp.NewNodeInfo("node1", "test", "127.0.0.1:8080", "", nil), nil)

	if err := app.AddComponent(&failStartComp{}); err != nil {
		t.Fatalf("add component: %v", err)
	}

	err := app.Start()
	if err == nil || err.Error() != "start failed" {
		t.Fatalf("start application err = %v, want start failed", err)
	}
}

//------------------------------ benchmark --------------------------------

// BenchmarkApplication_StartStop_OneComponent 测试单组件的启动/停止开销
func BenchmarkApplication_StartStop_OneComponent(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		app := NewApplication(kkapp.NewNodeInfo("node1", "test", "127.0.0.1:8080", "", nil), nil)
		if err := app.AddComponent(&benchComp{}); err != nil {
			b.Fatalf("add component: %v", err)
		}
		if err := app.Start(); err != nil {
			b.Fatalf("start application: %v", err)
		}
		// 等待 Application 收到 Started，避免 Stop 时还是 starting 状态
		time.Sleep(1 * time.Millisecond)
		if err := app.Stop(); err != nil {
			b.Fatalf("stop application: %v", err)
		}
	}
}

// BenchmarkApplication_StartStop_ManyComponents 测试多组件场景的启动/停止开销
func BenchmarkApplication_StartStop_ManyComponents(b *testing.B) {
	const compCount = 50
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		app := NewApplication(kkapp.NewNodeInfo("node1", "test", "127.0.0.1:8080", "", nil), nil)
		for j := 0; j < compCount; j++ {
			// 为避免重复组件名导致 ErrComponentAlreadyAdded，为每个组件生成唯一名称
			if err := app.AddComponent(&benchComp{name: fmt.Sprintf("bench-%d", j)}); err != nil {
				b.Fatalf("add component: %v", err)
			}
		}
		if err := app.Start(); err != nil {
			b.Fatalf("start application: %v", err)
		}
		// 等待 Application 收到 Started，避免 Stop 时还是 starting 状态
		time.Sleep(1 * time.Millisecond)
		if err := app.Stop(); err != nil {
			b.Fatalf("stop application: %v", err)
		}
	}
}

// BenchmarkApplication_AddComponent 测试单组件添加性能（包含 Application 创建开销）
func BenchmarkApplication_AddComponent(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		app := NewApplication(kkapp.NewNodeInfo("node1", "test", "127.0.0.1:8080", "", nil), nil)
		if err := app.AddComponent(&benchComp{}); err != nil {
			b.Fatalf("add component: %v", err)
		}
	}
}
