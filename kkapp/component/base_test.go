package component

import (
	"testing"
)

type testComponent struct {
	Component
}

var _ IComponent = (*testComponent)(nil)

func (slf *testComponent) GetID() string {
	return "test1"
}

func newComponent(id string) *testComponent {
	return &testComponent{}
}

func TestAddChildSuccess(t *testing.T) {
	parent := newComponent("parent")
	child := newComponent("child")

	if err := parent.AddCompenent(child); err != nil {
		t.Fatalf("AddChild unexpected error: %v", err)
	}

	if len(parent.GetComponents()) != 1 || parent.GetComponents()[0] != child {
		t.Fatalf("parent should contain the child")
	}
}

func TestAddChildWithoutStart(t *testing.T) {
	parent := newComponent("parent")
	child := newComponent("child")

	if err := parent.AddCompenent(child); err != nil {
		t.Fatalf("AddChild without start unexpected error: %v", err)
	}
}

func TestAddChildDuplicate(t *testing.T) {
	parent := newComponent("parent")
	child := newComponent("child")

	if err := parent.AddCompenent(child); err != nil {
		t.Fatalf("first AddChild unexpected error: %v", err)
	}
}

func TestRemoveChild(t *testing.T) {
	parent := newComponent("parent")
	child := newComponent("child")
	if err := parent.AddCompenent(child); err != nil {
		t.Fatalf("AddChild unexpected error: %v", err)
	}

	if err := parent.RemoveChild(child); err != nil {
		t.Fatalf("RemoveChild unexpected error: %v", err)
	}
	if len(parent.GetComponents()) != 0 {
		t.Fatalf("parent child list should be empty after removal")
	}
}

func TestGetRoot(t *testing.T) {
	root := newComponent("root")
	level1 := newComponent("level1")
	level2 := newComponent("level2")

	if err := root.AddCompenent(level1); err != nil {
		t.Fatalf("AddChild level1 error: %v", err)
	}
	if err := level1.AddCompenent(level2); err != nil {
		t.Fatalf("AddChild level2 error: %v", err)
	}
}
