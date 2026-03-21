package component

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/eventstream"
	"github.com/vvisun/kkdg/kkapp/faultreport"
	"github.com/vvisun/kkdg/utils/kkevent"
	"github.com/vvisun/kkdg/utils/kklog"
)

const terminatedFallbackDelay = 150 * time.Millisecond

func getPIDKey(pid *actor.PID) string {
	if pid == nil {
		return ""
	}
	// Address+Id should be unique enough for correlating Terminated->component.
	return fmt.Sprintf("%s|%s", pid.Address, pid.Id)
}

type appWatcher struct {
	app *Application

	// pidKey -> component name, used to map protoactor Terminated events back to a component.
	pidKeyToCompName map[string]string
	pidKeyMu         sync.RWMutex

	// faultHandledPID ensures we publish a component fault only once.
	// It is accessed from both:
	// - actor Receive loop (Terminated)
	// - protoactor EventStream callback (SupervisorEvent)
	faultHandledPID map[string]struct{}
	faultHandledMu  sync.Mutex

	// pendingTerminated holds fallback timers. If we receive Terminated but miss SupervisorEvent,
	// we will publish a minimal fault event after a short delay.
	pendingTerminated map[string]*time.Timer
	pendingMu         sync.Mutex

	// EventStream subscription id for supervision events.
	faultSub      *eventstream.Subscription
	faultEventMgr *kkevent.SpecEventManager[string, *faultreport.ComponentFaultEvent]

	// faultStopScheduled is used to schedule the stop of the application after a short delay.
	faultStopScheduled atomic.Bool
	faultRuleTable     *faultreport.RuleTable
}

func newAppWatcher(app *Application) *appWatcher {
	return &appWatcher{
		app:               app,
		pidKeyToCompName:  make(map[string]string),
		faultHandledPID:   make(map[string]struct{}),
		pendingTerminated: make(map[string]*time.Timer),
		faultEventMgr:     kkevent.NewSpecEventManager[string, *faultreport.ComponentFaultEvent](),
		faultRuleTable:    faultreport.NewRuleTable(),
	}
}

func (slf *appWatcher) supervisorStrategy() actor.SupervisorStrategy {
	return actor.NewOneForOneStrategy(
		0, 10*time.Second,
		func(_ any) actor.Directive {
			return actor.StopDirective
		},
	)
}

func (slf *appWatcher) watchComponent(ctx actor.Context, pid *actor.PID, compName string) {
	// Watch component actor lifecycle so we can receive Terminated.
	ctx.Watch(pid)
	// Record mapping early so we can correlate Terminated even if OnStart panics/exits.
	pidKey := getPIDKey(pid)
	slf.pidKeyMu.Lock()
	slf.pidKeyToCompName[pidKey] = compName
	slf.pidKeyMu.Unlock()
}

func (slf *appWatcher) onStopped() {
	// Stop listening to supervision events.
	if slf.faultSub != nil {
		actorSystem := slf.app.actorFramework.GetActorSystem()
		actorSystem.EventStream.Unsubscribe(slf.faultSub)
		slf.faultSub = nil
	}
	slf.cleanupSupervisorEvent()
}

func (slf *appWatcher) decideFaultAction(eData *faultreport.ComponentFaultEvent) faultreport.EFaultAction {
	action, ok := slf.faultRuleTable.GetRule(eData.ComponentName)
	if ok {
		return action
	}
	// 如果外部没有配置，则默认停止应用
	return faultreport.FaultActionStopApp
}

func (slf *appWatcher) onComponentFault(ctx actor.Context, eData *faultreport.ComponentFaultEvent, action faultreport.EFaultAction) {
	pidKey := eData.TerminatedPIDKey

	// Cancel any pending Terminated fallback for this pid.
	slf.pendingMu.Lock()
	if t := slf.pendingTerminated[pidKey]; t != nil {
		t.Stop()
		delete(slf.pendingTerminated, pidKey)
	}
	slf.pendingMu.Unlock()

	scheduleStopApp := func() {
		if slf.faultStopScheduled.CompareAndSwap(false, true) {
			time.AfterFunc(500*time.Millisecond, func() {
				_ = slf.app.Stop()
			})
		}
	}

	switch action {
	case faultreport.FaultActionStopApp:
		scheduleStopApp()
	case faultreport.FaultActionStopComp:
		// 组件已经终止，这里无需额外应用级动作。
	default:
		kklog.Warnf("%s unknown fault action %d for component %s", slf.app.logTag(), action, eData.ComponentName)
	}
}

func (slf *appWatcher) onTerminated(ctx actor.Context) {
	msg := ctx.Message().(*actor.Terminated)

	pidKey := getPIDKey(msg.Who)
	slf.pidKeyMu.RLock()
	compName := slf.pidKeyToCompName[pidKey]
	slf.pidKeyMu.RUnlock()
	if compName == "" {
		// Unknown PID (already cleared mapping or watcher received late message).
		return
	}

	// Terminated is a fallback signal. Prefer SupervisorEvent (with failure reason) when available.
	// Delay this publication so the SupervisorEvent can arrive first.
	why := msg.GetWhy()

	slf.pendingMu.Lock()
	// if already scheduled, don't schedule twice
	if _, ok := slf.pendingTerminated[pidKey]; ok {
		slf.pendingMu.Unlock()
		return
	}
	timer := time.AfterFunc(terminatedFallbackDelay, func() {
		slf.pendingMu.Lock()
		delete(slf.pendingTerminated, pidKey)
		slf.pendingMu.Unlock()

		curState := atomic.LoadInt64(&slf.app.state)
		if curState == ComponentStateStopping || curState == ComponentStateStopped {
			return
		}
		if !slf.markFaultHandled(pidKey) {
			return
		}

		// best-effort re-read comp name
		slf.pidKeyMu.RLock()
		cn := slf.pidKeyToCompName[pidKey]
		slf.pidKeyMu.RUnlock()
		if cn == "" {
			cn = compName
		}

		eData := &faultreport.ComponentFaultEvent{
			NodeID:           slf.app.GetNodeId(),
			NodeType:         slf.app.GetNodeType(),
			ComponentName:    cn,
			TerminatedPIDKey: pidKey,
			TerminatedWhy:    why,
		}
		action := slf.decideFaultAction(eData)
		eData.FaultAction = action
		slf.faultEventMgr.Publish(faultreport.EventKeyComponentFault, eData)
		slf.onComponentFault(ctx, eData, action)
	})
	slf.pendingTerminated[pidKey] = timer
	slf.pendingMu.Unlock()
}

func (slf *appWatcher) initSupervisorEvent(ctx actor.Context) {
	if slf.faultSub != nil {
		return
	}

	// Subscribe protoactor-go supervision events so we can capture failure context
	// (e.g. panic value) for component "core severity" decisions.
	// This subscription is process-local; it decouples the decision point (Application)
	// from the execution point(s) (listeners that may broadcast maintenance).
	actorSystem := slf.app.actorFramework.GetActorSystem()
	slf.faultSub = actorSystem.EventStream.SubscribeWithPredicate(func(evt interface{}) {
		// Skip while stopping/stopped to avoid duplicated or late events.
		curState := atomic.LoadInt64(&slf.app.state)
		if curState == ComponentStateStopping || curState == ComponentStateStopped {
			return
		}

		supervisorEvent := evt.(*actor.SupervisorEvent)
		if supervisorEvent == nil || supervisorEvent.Child == nil {
			return
		}

		pidKey := getPIDKey(supervisorEvent.Child)
		if pidKey == "" {
			return
		}

		slf.pidKeyMu.RLock()
		compName := slf.pidKeyToCompName[pidKey]
		slf.pidKeyMu.RUnlock()
		if compName == "" {
			return
		}

		if !slf.markFaultHandled(pidKey) {
			return
		}

		reasonStr, isPanic := faultreport.NormalizeFailureReason(supervisorEvent.Reason)

		eData := &faultreport.ComponentFaultEvent{
			NodeID:           slf.app.GetNodeId(),
			NodeType:         slf.app.GetNodeType(),
			ComponentName:    compName,
			TerminatedPIDKey: pidKey,

			FailureReason:       supervisorEvent.Reason,
			FailureReasonString: reasonStr,
			FailureDirective:    supervisorEvent.Directive,
			IsPanic:             isPanic,
		}
		action := slf.decideFaultAction(eData)
		eData.FaultAction = action
		slf.faultEventMgr.Publish(faultreport.EventKeyComponentFault, eData)
		slf.onComponentFault(ctx, eData, action)

	}, func(evt interface{}) bool {
		_, ok := evt.(*actor.SupervisorEvent)
		return ok
	})
}

func (slf *appWatcher) cleanupSupervisorEvent() {
	// stop & clear pending terminated fallbacks
	slf.pendingMu.Lock()
	for k, t := range slf.pendingTerminated {
		if t != nil {
			t.Stop()
		}
		delete(slf.pendingTerminated, k)
	}
	slf.pendingMu.Unlock()

	slf.pidKeyMu.Lock()
	for k := range slf.pidKeyToCompName {
		delete(slf.pidKeyToCompName, k)
	}
	slf.pidKeyMu.Unlock()

	slf.faultHandledMu.Lock()
	for k := range slf.faultHandledPID {
		delete(slf.faultHandledPID, k)
	}
	slf.faultHandledMu.Unlock()

	if slf.faultSub != nil {
		actorSystem := slf.app.actorFramework.GetActorSystem()
		actorSystem.EventStream.Unsubscribe(slf.faultSub)
		slf.faultSub = nil
	}
}

func (slf *appWatcher) markFaultHandled(pidKey string) bool {
	if pidKey == "" {
		return false
	}
	slf.faultHandledMu.Lock()
	defer slf.faultHandledMu.Unlock()
	if _, ok := slf.faultHandledPID[pidKey]; ok {
		return false
	}
	slf.faultHandledPID[pidKey] = struct{}{}
	return true
}
