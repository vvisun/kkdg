package component

import (
	"errors"
	"testing"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkerrors"
)

func TestApplication_New(t *testing.T) {
	nodeInfo := kkapp.NewNodeInfo("node1", "gate", "127.0.0.1:8080", "", nil)
	app := NewApplication(nodeInfo)
	if app == nil {
		t.Fatal("NewApplication returned nil")
	}
	if app.GetNodeId() != "node1" {
		t.Errorf("GetNodeId() = %q, want node1", app.GetNodeId())
	}
	if app.GetNodeType() != "gate" {
		t.Errorf("GetNodeType() = %q, want gate", app.GetNodeType())
	}
	if app.GetNodeInfo() != nodeInfo {
		t.Error("GetNodeInfo() != nodeInfo")
	}
	if app.GetActorSystem() == nil {
		t.Error("GetActorSystem() should not be nil")
	}
	if len(app.GetComponents()) != 0 {
		t.Errorf("GetComponents() len = %d, want 0", len(app.GetComponents()))
	}
}

func TestApplication_AddComponent_Success(t *testing.T) {
	app := NewApplication(kkapp.NewNodeInfo("n1", "t1", "a", "", nil))
	comp := &Component{id: "comp1"}
	if err := app.AddComponent(comp); err != nil {
		t.Fatalf("AddComponent: %v", err)
	}
	if !app.HasComponent(comp) {
		t.Error("HasComponent(comp) should be true")
	}
	comps := app.GetComponents()
	if len(comps) != 1 || comps[0] != comp {
		t.Errorf("GetComponents() = %v", comps)
	}
	if comp.GetApplication() != app {
		t.Error("comp.GetApplication() != app")
	}
}

func TestApplication_AddComponent_Duplicate(t *testing.T) {
	app := NewApplication(kkapp.NewNodeInfo("n1", "t1", "a", "", nil))
	comp := &Component{id: "comp1"}
	if err := app.AddComponent(comp); err != nil {
		t.Fatalf("first AddComponent: %v", err)
	}
	err := app.AddComponent(comp)
	if err != kkerrors.ErrComponentAlreadyAdded {
		t.Errorf("second AddComponent = %v, want ErrComponentAlreadyAdded", err)
	}
}

func TestApplication_AddComponent_SameIDDifferentInstance(t *testing.T) {
	app := NewApplication(kkapp.NewNodeInfo("n1", "t1", "a", "", nil))
	comp1 := &Component{id: "comp1"}
	comp2 := &Component{id: "comp1"}
	if err := app.AddComponent(comp1); err != nil {
		t.Fatalf("AddComponent comp1: %v", err)
	}
	err := app.AddComponent(comp2)
	if err != kkerrors.ErrComponentAlreadyAdded {
		t.Errorf("AddComponent same ID = %v, want ErrComponentAlreadyAdded", err)
	}
}

func TestApplication_AddComponent_InitFail(t *testing.T) {
	app := NewApplication(kkapp.NewNodeInfo("n1", "t1", "a", "", nil))
	failComp := &failInitComponent{id: "fail"}
	err := app.AddComponent(failComp)
	if err != errInitFailed {
		t.Errorf("AddComponent(init fail) = %v, want errInitFailed", err)
	}
	if app.HasComponent(failComp) {
		t.Error("component should not be added when Init fails")
	}
	comps := app.GetComponents()
	if len(comps) != 0 {
		t.Errorf("GetComponents() len = %d, want 0", len(comps))
	}
}

var errInitFailed = errors.New("init failed")

type failInitComponent struct {
	id string
}

func (c *failInitComponent) GetCompName() string           { return c.id }
func (c *failInitComponent) SetApplication(_ IApplication) {}
func (c *failInitComponent) GetApplication() IApplication  { return nil }
func (c *failInitComponent) Init() error                   { return errInitFailed }
func (c *failInitComponent) Start() error                  { return nil }
func (c *failInitComponent) Stop() error                   { return nil }
func (c *failInitComponent) Equal(other IComponent) bool   { return IsEqual(c, other) }

func TestApplication_Start_Stop(t *testing.T) {
	app := NewApplication(kkapp.NewNodeInfo("n1", "t1", "a", "", nil))
	comp := &Component{id: "c1"}
	if err := app.AddComponent(comp); err != nil {
		t.Fatalf("AddComponent: %v", err)
	}
	if err := app.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := app.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestApplication_Start_ComponentFails(t *testing.T) {
	app := NewApplication(kkapp.NewNodeInfo("n1", "t1", "a", "", nil))
	comp := &failStartComponent{id: "fs"}
	if err := app.AddComponent(comp); err != nil {
		t.Fatalf("AddComponent: %v", err)
	}
	err := app.Start()
	if err != errStartFailed {
		t.Errorf("Start = %v, want errStartFailed", err)
	}
}

var errStartFailed = errors.New("start failed")

type failStartComponent struct {
	id string
}

func (c *failStartComponent) GetCompName() string           { return c.id }
func (c *failStartComponent) SetApplication(_ IApplication) {}
func (c *failStartComponent) GetApplication() IApplication  { return nil }
func (c *failStartComponent) Init() error                   { return nil }
func (c *failStartComponent) Start() error                  { return errStartFailed }
func (c *failStartComponent) Stop() error                   { return nil }
func (c *failStartComponent) Equal(other IComponent) bool   { return IsEqual(c, other) }

func TestApplication_HasComponent_Empty(t *testing.T) {
	app := NewApplication(kkapp.NewNodeInfo("n1", "t1", "a", "", nil))
	comp := &Component{id: "c1"}
	if app.HasComponent(comp) {
		t.Error("HasComponent on empty app should be false")
	}
}
