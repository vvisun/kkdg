package component

import (
	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/utils/kkevent"
)

const (
	// EventKeyComponentFault is used to broadcast component actor termination events.
	EventKeyComponentFault = "component_fault"
)

// ComponentFaultEvent is the payload published when a component actor terminates.
//
// This event bus is in-process and is meant to decouple:
// - the decision/orchestration point (Application reception point)
// - the execution point(s) that react (maintenance mode, metrics, etc.)
type ComponentFaultEvent struct {
	NodeID        string
	NodeType      string
	ComponentName string

	TerminatedWhy    actor.TerminatedReason
	TerminatedPIDKey string
}

// GlobalFaultEventMgr is the process-level event manager for component fault events.
var GlobalFaultEventMgr = kkevent.NewSpecEventManager[string, *ComponentFaultEvent]()

