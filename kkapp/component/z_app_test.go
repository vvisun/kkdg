package component

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/faultreport"
	"github.com/vvisun/kkdg/kkapp/kkactor"
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
	time.Sleep(2 * time.Second)
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
	af := kkactor.NewActorFramework()
	app := NewApplication(kkapp.NewNodeInfo("node1", "test", "127.0.0.1:8080", ""), af, kkapp.ApplyOptions())
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
	af := kkactor.NewActorFramework()
	app := NewApplication(kkapp.NewNodeInfo("node1", "test", "127.0.0.1:8080", ""), af, kkapp.ApplyOptions())

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
	af := kkactor.NewActorFramework()
	app := NewApplication(kkapp.NewNodeInfo("node1", "test", "127.0.0.1:8080", ""), af, kkapp.ApplyOptions())

	if err := app.Stop(); err != kkerrors.ErrAppNotStarted {
		t.Fatalf("stop application when not started should return ErrAppNotStarted, got: %v", err)
	}
}

// 测试非 None 状态下添加组件
func TestApplication_AddComponent_NotNoneState(t *testing.T) {
	af := kkactor.NewActorFramework()
	app := NewApplication(kkapp.NewNodeInfo("node1", "test", "127.0.0.1:8080", ""), af, kkapp.ApplyOptions())

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
	af := kkactor.NewActorFramework()
	app := NewApplication(kkapp.NewNodeInfo("node1", "test", "127.0.0.1:8080", ""), af, kkapp.ApplyOptions())

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
	af := kkactor.NewActorFramework()
	app := NewApplication(kkapp.NewNodeInfo("node1", "test", "127.0.0.1:8080", ""), af, kkapp.ApplyOptions())

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
	af := kkactor.NewActorFramework()
	app := NewApplication(kkapp.NewNodeInfo("node1", "test", "127.0.0.1:8080", ""), af, kkapp.ApplyOptions())

	if err := app.AddComponent(&failStartComp{}); err != nil {
		t.Fatalf("add component: %v", err)
	}

	err := app.Start()
	if err == nil || err.Error() != "start failed" {
		t.Fatalf("start application err = %v, want start failed", err)
	}
}

//--------------------------------------------------------------------------------
// fault event tests
//--------------------------------------------------------------------------------

type panicOnStringComp struct {
	Component
}

func (c *panicOnStringComp) GetCompName() string { return "panic_comp" }
func (c *panicOnStringComp) OnInit() error       { return nil }
func (c *panicOnStringComp) OnStart() error      { return nil }
func (c *panicOnStringComp) OnStop() error       { return nil }
func (c *panicOnStringComp) Receive(ctx actor.Context) {
	switch ctx.Message().(type) {
	case string:
		panic("boom")
	case *actor.Stopping:
		_ = c.OnStop()
	}
}

func TestApplication_FaultEvent_SupervisorEventWinsAndCancelsTerminatedFallback(t *testing.T) {
	af := kkactor.NewActorFramework()
	app := NewApplication(kkapp.NewNodeInfo("node1", "test", "127.0.0.1:8080", ""), af, kkapp.ApplyOptions())
	if err := app.AddComponent(&panicOnStringComp{}); err != nil {
		t.Fatalf("add component: %v", err)
	}
	if err := app.Start(); err != nil {
		t.Fatalf("start application: %v", err)
	}
	defer func() { _ = app.Stop() }()

	evtCh := make(chan *faultreport.ComponentFaultEvent, 10)
	app.GetFaultEventMgr().Subscribe(faultreport.EventKeyComponentFault, func(e *faultreport.ComponentFaultEvent) {
		evtCh <- e
	})

	pid := app.GetCompPID("panic_comp")
	if pid == nil {
		t.Fatal("panic_comp pid should not be nil")
	}

	// trigger panic -> should publish via SupervisorEvent (with FailureReasonString/IsPanic)
	app.GetActorFramework().GetActorSystem().Root.Send(pid, "trigger")

	var first *faultreport.ComponentFaultEvent
	select {
	case first = <-evtCh:
	case <-time.After(2 * time.Second):
		t.Fatal("expected fault event, got timeout")
	}
	if first.ComponentName != "panic_comp" {
		t.Fatalf("event ComponentName=%q, want %q", first.ComponentName, "panic_comp")
	}
	if first.FailureReasonString == "" {
		t.Fatalf("expected FailureReasonString to be set for supervisor event")
	}
	if !first.IsPanic {
		t.Fatalf("expected IsPanic=true for panic value")
	}
	if first.FaultAction != faultreport.FaultActionStopApp {
		t.Fatalf("event FaultAction=%v, want %v", first.FaultAction, faultreport.FaultActionStopApp)
	}

	// Wait longer than terminated fallback delay; should not receive a second event.
	time.Sleep(terminatedFallbackDelay + 200*time.Millisecond)

	select {
	case extra := <-evtCh:
		t.Fatalf("unexpected extra fault event after fallback window: %+v", extra)
	default:
	}
}

func TestApplication_FaultEvent_TerminatedFallbackPublishesWhenNoSupervisorEvent(t *testing.T) {
	af := kkactor.NewActorFramework()
	app := NewApplication(kkapp.NewNodeInfo("node1", "test", "127.0.0.1:8080", ""), af, kkapp.ApplyOptions())
	if err := app.AddComponent(&TestComp1{}); err != nil {
		t.Fatalf("add component: %v", err)
	}
	if err := app.Start(); err != nil {
		t.Fatalf("start application: %v", err)
	}
	defer func() { _ = app.Stop() }()

	var got atomic.Int32
	evtCh := make(chan *faultreport.ComponentFaultEvent, 10)
	app.GetFaultEventMgr().Subscribe(faultreport.EventKeyComponentFault, func(e *faultreport.ComponentFaultEvent) {
		got.Add(1)
		evtCh <- e
	})

	pid := app.GetCompPID("test1")
	if pid == nil {
		t.Fatal("test1 pid should not be nil")
	}

	// Stop the child explicitly (no failure), should lead to Terminated and fallback publication.
	app.GetActorFramework().GetActorSystem().Root.Stop(pid)

	select {
	case e := <-evtCh:
		if e.ComponentName != "test1" {
			t.Fatalf("event ComponentName=%q, want %q", e.ComponentName, "test1")
		}
		if e.TerminatedPIDKey == "" {
			t.Fatalf("expected TerminatedPIDKey to be set")
		}
		if e.FailureReasonString != "" || e.IsPanic {
			t.Fatalf("expected no FailureReason fields for terminated fallback, got reason=%q isPanic=%v", e.FailureReasonString, e.IsPanic)
		}
		if e.FaultAction != faultreport.FaultActionStopApp {
			t.Fatalf("event FaultAction=%v, want %v", e.FaultAction, faultreport.FaultActionStopApp)
		}
		// TerminatedWhy should be populated for fallback
		_ = e.TerminatedWhy
	case <-time.After(terminatedFallbackDelay + 2*time.Second):
		t.Fatalf("expected terminated fallback fault event, got timeout (count=%d)", got.Load())
	}
}

func TestApplication_FaultEvent_ActionFromOptions_DoesNotStopAppWhenStopComp(t *testing.T) {
	app := NewApplication(
		kkapp.NewNodeInfo("node1", "test", "127.0.0.1:8080", ""),
		kkactor.NewActorFramework(),
		kkapp.ApplyOptions(
			kkapp.WithFaultAction("panic_comp", faultreport.FaultActionStopComp),
		),
	)
	if err := app.AddComponent(&panicOnStringComp{}); err != nil {
		t.Fatalf("add component: %v", err)
	}
	if err := app.Start(); err != nil {
		t.Fatalf("start application: %v", err)
	}
	defer func() { _ = app.Stop() }()

	evtCh := make(chan *faultreport.ComponentFaultEvent, 10)
	app.GetFaultEventMgr().Subscribe(faultreport.EventKeyComponentFault, func(e *faultreport.ComponentFaultEvent) {
		evtCh <- e
	})

	pid := app.GetCompPID("panic_comp")
	if pid == nil {
		t.Fatal("panic_comp pid should not be nil")
	}
	app.GetActorFramework().GetActorSystem().Root.Send(pid, "trigger")

	select {
	case e := <-evtCh:
		if e.FaultAction != faultreport.FaultActionStopComp {
			t.Fatalf("event FaultAction=%v, want %v", e.FaultAction, faultreport.FaultActionStopComp)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected fault event, got timeout")
	}

	// Give enough time to observe whether stop-app action was incorrectly triggered.
	time.Sleep(700 * time.Millisecond)
	if atomic.LoadInt64(&app.state) != ComponentStateStarted {
		t.Fatalf("application state=%s, want %s", GetStateName(atomic.LoadInt64(&app.state)), GetStateName(ComponentStateStarted))
	}
}
