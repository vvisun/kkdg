package faultreport

import "github.com/asynkron/protoactor-go/actor"

const (
	// EventKeyComponentFault is the payload published when a component actor terminates.
	// EventKeyComponentFault 是组件故障事件。当组件actor终止时，会发布这个事件。
	//
	//  外部可以通过订阅这个事件来获取组件故障信息，然后【广播到客户端】。
	//  只是为了客户端体验更佳，是否订阅并处理不强制要求。因为服务停止后客户端会全部掉线，再重连登录时登录服会自动反馈“维护中”。
	EventKeyComponentFault = "component_fault"
)

type ComponentFaultEvent struct {
	FaultAction      EFaultAction
	NodeID           string
	NodeType         string
	ComponentName    string
	TerminatedPIDKey string

	// Failure fields are derived from protoactor-go supervision events.
	// They are best-effort context: Reason is usually the panic value.
	FailureReason       any
	FailureReasonString string
	FailureDirective    actor.Directive
	IsPanic             bool

	TerminatedWhy actor.TerminatedReason
}
