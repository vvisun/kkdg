package faultreport

import "github.com/asynkron/protoactor-go/actor"

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
	IsPanic             bool

	TerminatedWhy    actor.TerminatedReason
	TerminatedPIDKey string
}
