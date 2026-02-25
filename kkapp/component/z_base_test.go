package component

import (
	"testing"

	"github.com/vvisun/kkdg/kkapp"
)

func TestComponent_Lifecycle(t *testing.T) {
	c := &Component{id: "test-comp"}
	if c.GetID() != "test-comp" {
		t.Errorf("GetID() = %q, want test-comp", c.GetID())
	}
	if c.GetApplication() != nil {
		t.Errorf("GetApplication() = %v, want nil", c.GetApplication())
	}
	if err := c.Init(); err != nil {
		t.Errorf("Init() = %v", err)
	}
	if err := c.Start(); err != nil {
		t.Errorf("Start() = %v", err)
	}
	if err := c.Stop(); err != nil {
		t.Errorf("Stop() = %v", err)
	}
}

func TestComponent_SetApplication(t *testing.T) {
	c := &Component{id: "c1"}
	app := NewApplication(mustNodeInfo("n1", "t1"))
	c.SetApplication(app)
	if c.GetApplication() != app {
		t.Error("GetApplication() != app after SetApplication")
	}
}

func TestIsEqual(t *testing.T) {
	c1 := &Component{id: "id1"}
	c2 := &Component{id: "id2"}
	c3 := &Component{id: "id1"}

	if !IsEqual(c1, c1) {
		t.Error("IsEqual(same, same) should be true")
	}
	if !IsEqual(c1, c3) {
		t.Error("IsEqual(c1, c3) with same ID should be true")
	}
	if IsEqual(c1, c2) {
		t.Error("IsEqual(c1, c2) with different ID should be false")
	}
	if IsEqual(c1, nil) {
		t.Error("IsEqual(c1, nil) should be false")
	}
	if IsEqual(nil, c1) {
		t.Error("IsEqual(nil, c1) should be false")
	}
	if IsEqual(nil, nil) {
		t.Error("IsEqual(nil, nil) should be false")
	}
}

func TestGetComponentName(t *testing.T) {
	c := &Component{id: "gate1"}
	name := GetComponentName(c)
	if name == "" {
		t.Error("GetComponentName should not return empty")
	}
	if name != "Component_gate1" {
		t.Logf("GetComponentName = %q (struct name may vary)", name)
	}
}

func mustNodeInfo(nodeId, nodeType string) *kkapp.NodeInfo {
	return kkapp.NewNodeInfo(nodeId, nodeType, "127.0.0.1:0", "", nil)
}
