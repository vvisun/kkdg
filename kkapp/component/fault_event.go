package component

import (
	"fmt"
	"runtime"

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

	// Failure fields are derived from protoactor-go supervision events.
	// They are best-effort context: Reason is usually the panic value.
	FailureReason       any
	FailureReasonString string
	FailureDirective    actor.Directive
	IsPanic              bool

	TerminatedWhy    actor.TerminatedReason
	TerminatedPIDKey string
}

// GlobalFaultEventMgr is the process-level event manager for component fault events.
var GlobalFaultEventMgr = kkevent.NewSpecEventManager[string, *ComponentFaultEvent]()

func normalizeFailureReason(reason any) (reasonStr string, isPanic bool) {
	if reason == nil {
		return "", false
	}

	// protoactor-go typically passes the panic value as the "reason".
	// runtime.Error is a common case.
	if _, ok := reason.(runtime.Error); ok {
		return fmt.Sprintf("%v", reason), true
	}

	// An error is not necessarily a panic, but still provides useful content.
	if _, ok := reason.(error); ok {
		return fmt.Sprintf("%v", reason), false
	}

	// For non-error/non-runtime.Error types, we treat it as a panic-ish payload.
	return fmt.Sprintf("%v", reason), true
}

