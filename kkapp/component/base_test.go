package component

import (
	"errors"
	"testing"

	"github.com/vvisun/kkdg/kkerrors"
)

func newComponent(id string) *Component {
	return &Component{id: id}
}

func TestAddChildSuccess(t *testing.T) {
	parent := newComponent("parent")
	child := newComponent("child")

	if err := parent.AddChild(child, true); err != nil {
		t.Fatalf("AddChild unexpected error: %v", err)
	}

	if child.GetParent() != parent {
		t.Fatalf("child parent not set")
	}
	if len(parent.GetChildrens()) != 1 || parent.GetChildrens()[0] != child {
		t.Fatalf("parent should contain the child")
	}
}

func TestAddChildWithoutStart(t *testing.T) {
	parent := newComponent("parent")
	child := newComponent("child")

	if err := parent.AddChild(child, false); err != nil {
		t.Fatalf("AddChild without start unexpected error: %v", err)
	}
}

func TestAddChildAlreadyHasParent(t *testing.T) {
	parent1 := newComponent("p1")
	parent2 := newComponent("p2")
	child := newComponent("child")
	// Simulate existing parent relationship.
	child.parent = parent1

	if err := parent2.AddChild(child, true); !errors.Is(err, kkerrors.ErrComponentAlreadySetParent) {
		t.Fatalf("expected ErrComponentAlreadySetParent, got %v", err)
	}
}

func TestAddChildDuplicate(t *testing.T) {
	parent := newComponent("parent")
	child := newComponent("child")

	if err := parent.AddChild(child, true); err != nil {
		t.Fatalf("first AddChild unexpected error: %v", err)
	}
	if err := parent.AddChild(child, true); !errors.Is(err, kkerrors.ErrComponentAlreadySetParent) {
		t.Fatalf("expected ErrComponentAlreadySetParent, got %v", err)
	}
}

func TestRemoveChild(t *testing.T) {
	parent := newComponent("parent")
	child := newComponent("child")
	if err := parent.AddChild(child, true); err != nil {
		t.Fatalf("AddChild unexpected error: %v", err)
	}

	if err := parent.RemoveChild(child); err != nil {
		t.Fatalf("RemoveChild unexpected error: %v", err)
	}
	if child.GetParent() != nil {
		t.Fatalf("child parent should be nil after removal")
	}
	if len(parent.GetChildrens()) != 0 {
		t.Fatalf("parent child list should be empty after removal")
	}
}

func TestGetRoot(t *testing.T) {
	root := newComponent("root")
	level1 := newComponent("level1")
	level2 := newComponent("level2")

	if err := root.AddChild(level1, true); err != nil {
		t.Fatalf("AddChild level1 error: %v", err)
	}
	if err := level1.AddChild(level2, true); err != nil {
		t.Fatalf("AddChild level2 error: %v", err)
	}

	if got := level2.GetRoot(); got != root {
		t.Fatalf("expected root component, got %v", got.GetID())
	}
}
