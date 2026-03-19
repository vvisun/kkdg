package component

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/eventstream"
	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/faultreport"
	"github.com/vvisun/kkdg/kkapp/kkactor"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kkevent"
	"github.com/vvisun/kkdg/utils/kklog"
)

const terminatedFallbackDelay = 150 * time.Millisecond

var (
	defaultActorFramework *kkactor.ActorFramework
	onceActorFramework    sync.Once
)

// NewApplication中actorFramework参数为nil时，会使用该默认的全局ActorFramework
func getGlobalActorFramework() *kkactor.ActorFramework {
	onceActorFramework.Do(func() {
		defaultActorFramework = kkactor.NewActorFramework(kkactor.NewActorLocator(), kkactor.NewActorSystem())
	})
	return defaultActorFramework
}

// new application.
//
//	each application is a node, a actor.
//	if af is nil, will use default getGlobalActorFramework()
func NewApplication(nodeInfo *kkapp.NodeInfo, af *kkactor.ActorFramework, opts kkapp.AppOptions) *Application {
	if nodeInfo == nil {
		// 启动期间的异常装配直接panic，不然反而将隐含问题带到了运行期间，造成不可预测的错误
		kklog.PanicLog("nodeInfo is nil")
	}
	if af == nil {
		kklog.Infof("[kkapp] (nodeId: %s, nodeType: %s) new application actorFramework is nil, use default", nodeInfo.GetNodeId(), nodeInfo.GetNodeType())
		af = getGlobalActorFramework()
	}
	kkapp.CheckOptions(&opts)
	af.GetLocator().AddNode(nodeInfo)
	app := &Application{
		nodeInfo:          nodeInfo,
		actorFramework:    af,
		state:             ComponentStateNone,
		compList:          make([]kkapp.IComponent, 0),
		opts:              &opts,
		pidKeyToCompName:  make(map[string]string),
		faultHandledPID:   make(map[string]struct{}),
		pendingTerminated: make(map[string]*time.Timer),
		faultEventMgr:     kkevent.NewSpecEventManager[string, *faultreport.ComponentFaultEvent](),
	}
	kklog.Infof("[kkapp] (nodeId: %s, nodeType: %s) new application", nodeInfo.GetNodeId(), nodeInfo.GetNodeType())
	return app
}

func getPIDKey(pid *actor.PID) string {
	if pid == nil {
		return ""
	}
	// Address+Id should be unique enough for correlating Terminated->component.
	return fmt.Sprintf("%s|%s", pid.Address, pid.Id)
}

type Application struct {
	nodeInfo       *kkapp.NodeInfo
	actorFramework *kkactor.ActorFramework
	pid            *actor.PID
	state          ComponentState
	compList       []kkapp.IComponent
	startResultCh  chan error
	mu             sync.RWMutex

	opts *kkapp.AppOptions

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
}

var _ kkapp.IApplication = (*Application)(nil)

func (slf *Application) GetCompName() string {
	return slf.nodeInfo.GetNodeId()
}

func (slf *Application) GetNodeInfo() *kkapp.NodeInfo {
	return slf.nodeInfo
}

func (slf *Application) GetNodeId() string {
	return slf.nodeInfo.GetNodeId()
}

func (slf *Application) GetNodeType() string {
	return slf.nodeInfo.GetNodeType()
}

func (slf *Application) GetConfigDir() string {
	return slf.opts.ConfigsDir
}

func (slf *Application) GetActorFramework() *kkactor.ActorFramework {
	return slf.actorFramework
}

func (slf *Application) GetPID() *actor.PID {
	return slf.pid
}

func (slf *Application) GetOptions() *kkapp.AppOptions {
	return slf.opts
}

func (slf *Application) GetFaultEventMgr() *kkevent.SpecEventManager[string, *faultreport.ComponentFaultEvent] {
	return slf.faultEventMgr
}

func (slf *Application) logTag() string {
	return fmt.Sprintf("[kkapp] (nodeId: %s, nodeType: %s)", slf.GetNodeId(), slf.GetNodeType())
}

func (slf *Application) curStateName() string {
	return GetStateName(atomic.LoadInt64(&slf.state))
}

func (slf *Application) GetCompPID(compName string) *actor.PID {
	id, err := kkactor.NewLucencyActorID(slf.GetNodeId(), compName)
	if err != nil {
		kklog.Debugf("%s get component %s pid error: %v", slf.logTag(), compName, err)
		return nil
	}
	pid, err := slf.actorFramework.GetLocator().GetActor(id)
	if err != nil {
		kklog.Debugf("%s get component %s pid error: %v", slf.logTag(), compName, err)
		return nil
	}
	return pid
}

func (slf *Application) Start() error {
	slf.opts.FaultRuleTable.Lock() // 启动后锁定规则表，后续不允许再修改
	if !atomic.CompareAndSwapInt64(&slf.state, ComponentStateNone, ComponentStateStarting) {
		kklog.Errorf("%s start fail. already started, state: %s", slf.logTag(), slf.curStateName())
		return kkerrors.ErrAppAlreadyStarted
	}
	startResultCh := make(chan error, 1)
	slf.mu.Lock()
	slf.startResultCh = startResultCh
	slf.mu.Unlock()
	kklog.Infof("%s starting", slf.logTag())

	// Ensure components (children) will not be restarted on failure.
	// When a component actor crashes, protoactor-go will apply the supervisor strategy defined here.
	// We use StopDirective to permanently stop the failed child instead of Restart.
	sup := actor.NewOneForOneStrategy(
		0, 10*time.Second,
		func(_ any) actor.Directive {
			return actor.StopDirective
		},
	)
	slf.pid = slf.actorFramework.GetActorSystem().Root.Spawn(
		actor.PropsFromFunc(
			slf.Receive,
			actor.WithSupervisor(sup),
		),
	)
	if slf.pid == nil {
		kklog.Errorf("%s spawn actor fail", slf.logTag())
		// 启动期间的异常装配直接panic，不然反而将隐含问题带到了运行期间，造成不可预测的错误
		kklog.PanicErr(kkerrors.ErrAppSpawnActorFailed)
	}
	id, err := kkactor.NewLucencyActorID(slf.GetNodeId(), slf.GetCompName())
	if err != nil {
		kklog.Errorf("%s add component %s error: %v", slf.logTag(), slf.GetCompName(), err)
		// 启动期间的异常装配直接panic，不然反而将隐含问题带到了运行期间，造成不可预测的错误
		kklog.PanicErr(err)
	}
	err = slf.actorFramework.GetLocator().AddActor(id, slf.pid)
	if err != nil {
		kklog.Errorf("%s add component %s error: %v", slf.logTag(), slf.GetCompName(), err)
		// 启动期间的异常装配直接panic，不然反而将隐含问题带到了运行期间，造成不可预测的错误
		kklog.PanicErr(err)
	}
	return <-startResultCh
}

func (slf *Application) Stop() error {
	if slf.pid == nil {
		kklog.Errorf("%s stop fail. not started, state: %s", slf.logTag(), slf.curStateName())
		return kkerrors.ErrAppNotStarted
	}
	if !atomic.CompareAndSwapInt64(&slf.state, ComponentStateStarted, ComponentStateStopping) {
		kklog.Errorf("%s stop fail. not started, state: %s", slf.logTag(), slf.curStateName())
		return kkerrors.ErrAppNotStarted
	}
	// 等待 Application actor 完全退出，否则进程可能在 Stopping/Stopped 未处理时就退出，看不到日志
	err := slf.actorFramework.GetActorSystem().Root.PoisonFuture(slf.pid).Wait()
	if err != nil {
		kklog.Errorf("%s stop error: %v", slf.logTag(), err)
		return err
	}
	atomic.CompareAndSwapInt64(&slf.state, ComponentStateStopping, ComponentStateStopped)
	slf.pid = nil
	return nil
}

// 将comp作为一个子actor添加到application中
//
// 注意:
//
//	-启动顺序和添加顺序相反，先添加的后启动（因为protoactor-go的actor启动顺序是先添加的后启动）；
//	-停止顺序和启动顺序相反，先启动的后停止（因为protoactor-go的actor停止顺序是先添加的后停止）；
//
// 最佳的应用层架构方式应该是，能做到所有组件的启动顺序可以任意调换，不需要考虑启动顺序。
// 因为组件应该尽量独立，只有在需要通信交互时，才需要也只需要 通过这个组件的actorID，进行消息投递。
func (slf *Application) AddComponent(comp kkapp.IComponent) error {
	if _, err := kkactor.NewLucencyActorID(slf.GetNodeId(), comp.GetCompName()); err != nil {
		kklog.Errorf("%s add component %s error: %v", slf.logTag(), comp.GetCompName(), err)
		return err
	}
	if slf.existsComponent(comp) {
		kklog.Errorf("%s already has component %s", slf.logTag(), comp.GetCompName())
		return kkerrors.ErrComponentAlreadyAdded
	}
	if atomic.LoadInt64(&slf.state) != ComponentStateNone {
		kklog.Errorf("%s add component %s fail. not none state, state: %s", slf.logTag(), comp.GetCompName(), slf.curStateName())
		return kkerrors.ErrAppAddCompMustInNoneState
	}

	comp.SetApplication(slf)
	err := comp.OnInit()
	if err != nil {
		kklog.Errorf("%s init component %s error: %v", slf.logTag(), comp.GetCompName(), err)
		return err
	}

	slf.mu.Lock()
	slf.compList = append(slf.compList, comp)
	slf.mu.Unlock()
	kklog.Infof("%s add component %s", slf.logTag(), comp.GetCompName())
	return nil
}

func (slf *Application) existsComponent(comp kkapp.IComponent) bool {
	slf.mu.RLock()
	defer slf.mu.RUnlock()
	for _, c := range slf.compList {
		if IsEqual(c, comp) {
			return true
		}
	}
	return false
}

func (slf *Application) onStarted(ctx actor.Context) {
	if atomic.LoadInt64(&slf.state) != ComponentStateStarting {
		kklog.Errorf("%s already started, state: %s", slf.logTag(), slf.curStateName())
		return //已经启动，直接返回
	}

	slf.initSupervisorEvent(ctx)

	slf.mu.RLock()
	comps := make([]kkapp.IComponent, len(slf.compList))
	copy(comps, slf.compList)
	slf.mu.RUnlock()

	for _, comp := range comps {
		props := actor.PropsFromFunc(comp.Receive)
		pid := ctx.Spawn(props)
		if pid == nil {
			kklog.Errorf("%s spawn component %s fail", slf.logTag(), comp.GetCompName())
			// 启动期间的异常装配直接panic，不然反而将隐含问题带到了运行期间，造成不可预测的错误
			kklog.PanicErr(kkerrors.ErrAppSpawnActorFailed)
		}

		comp.SetPID(pid)

		// Watch component actor lifecycle so we can receive Terminated.
		ctx.Watch(pid)
		// Record mapping early so we can correlate Terminated even if OnStart panics/exits.
		pidKey := getPIDKey(pid)
		slf.pidKeyMu.Lock()
		slf.pidKeyToCompName[pidKey] = comp.GetCompName()
		slf.pidKeyMu.Unlock()

		id, err := kkactor.NewLucencyActorID(slf.GetNodeId(), comp.GetCompName())
		if err != nil {
			kklog.Errorf("%s add component %s error: %v", slf.logTag(), comp.GetCompName(), err)
			// 启动期间的异常装配直接panic，不然反而将隐含问题带到了运行期间，造成不可预测的错误
			kklog.PanicErr(err)
		}
		err = slf.actorFramework.GetLocator().AddActor(id, pid)
		if err != nil {
			kklog.Errorf("%s add component %s error: %v", slf.logTag(), comp.GetCompName(), err)
			// 启动期间的异常装配直接panic，不然反而将隐含问题带到了运行期间，造成不可预测的错误
			kklog.PanicErr(err)
		}
		if err := comp.OnStart(); err != nil {
			kklog.Errorf("%s start component %s error: %v", slf.logTag(), comp.GetCompName(), err)
			atomic.CompareAndSwapInt64(&slf.state, ComponentStateStarting, ComponentStateStopping)
			slf.finishStart(err)
			ctx.Stop(ctx.Self())
			return
		}
	}
	atomic.CompareAndSwapInt64(&slf.state, ComponentStateStarting, ComponentStateStarted)
	kklog.Infof("%s started", slf.logTag())
	slf.finishStart(nil)
}

func (slf *Application) finishStart(err error) {
	slf.mu.Lock()
	startResultCh := slf.startResultCh
	slf.startResultCh = nil
	slf.mu.Unlock()
	if startResultCh == nil {
		return
	}
	startResultCh <- err
	close(startResultCh)
}

func (slf *Application) onStopped() {
	atomic.CompareAndSwapInt64(&slf.state, ComponentStateStopping, ComponentStateStopped)

	// Stop listening to supervision events.
	if slf.faultSub != nil {
		actorSystem := slf.actorFramework.GetActorSystem()
		actorSystem.EventStream.Unsubscribe(slf.faultSub)
		slf.faultSub = nil
	}

	id, _ := kkactor.NewLucencyActorID(slf.GetNodeId(), slf.GetCompName())
	slf.actorFramework.GetLocator().RemoveActor(id)
	slf.actorFramework.GetLocator().RemoveNode(slf.nodeInfo)
	slf.mu.Lock()
	slf.compList = make([]kkapp.IComponent, 0)
	slf.mu.Unlock()

	slf.cleanupSupervisorEvent()

	kklog.Infof("%s stopped", slf.logTag())
}

func (slf *Application) Receive(ctx actor.Context) {
	switch ctx.Message().(type) {
	case *actor.Started:
		slf.onStarted(ctx)
	case *actor.Terminated:
		slf.onTerminated(ctx)
	case *actor.Stopping:
		kklog.Infof("%s stopping", slf.logTag())
	case *actor.Stopped:
		slf.onStopped()
	case *actor.Restarting:
		kklog.Infof("%s restarting", slf.logTag())
	}
}

func (slf *Application) decideFaultAction(eData *faultreport.ComponentFaultEvent) faultreport.EFaultAction {
	action, ok := slf.opts.FaultRuleTable.GetRule(eData.ComponentName)
	if ok {
		return action
	}
	// 如果外部没有配置，则默认停止应用
	return faultreport.FaultActionStopApp
}

func (slf *Application) onComponentFault(ctx actor.Context, eData *faultreport.ComponentFaultEvent, action faultreport.EFaultAction) {
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
				_ = slf.Stop()
			})
		}
	}

	switch action {
	case faultreport.FaultActionStopApp:
		scheduleStopApp()
	case faultreport.FaultActionStopComp:
		// 组件已经终止，这里无需额外应用级动作。
	case faultreport.FaultActionRestartComp:
		// 当前策略不做组件自动重启，后续可在这里接入重启编排。
	default:
		kklog.Warnf("%s unknown fault action %d for component %s", slf.logTag(), action, eData.ComponentName)
	}
}

func (slf *Application) onTerminated(ctx actor.Context) {
	curState := atomic.LoadInt64(&slf.state)
	if curState == ComponentStateStopping || curState == ComponentStateStopped {
		// Normal shutdown path: ignore component termination while application is stopping/stopped.
		return
	}

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

		curState := atomic.LoadInt64(&slf.state)
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
			NodeID:           slf.GetNodeId(),
			NodeType:         slf.GetNodeType(),
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

func (slf *Application) initSupervisorEvent(ctx actor.Context) {
	if slf.faultSub != nil {
		return
	}

	// Subscribe protoactor-go supervision events so we can capture failure context
	// (e.g. panic value) for component "core severity" decisions.
	// This subscription is process-local; it decouples the decision point (Application)
	// from the execution point(s) (listeners that may broadcast maintenance).
	actorSystem := slf.actorFramework.GetActorSystem()
	slf.faultSub = actorSystem.EventStream.SubscribeWithPredicate(func(evt interface{}) {
		// Skip while stopping/stopped to avoid duplicated or late events.
		curState := atomic.LoadInt64(&slf.state)
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
			NodeID:           slf.GetNodeId(),
			NodeType:         slf.GetNodeType(),
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

func (slf *Application) cleanupSupervisorEvent() {
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
		actorSystem := slf.actorFramework.GetActorSystem()
		actorSystem.EventStream.Unsubscribe(slf.faultSub)
		slf.faultSub = nil
	}
}

func (slf *Application) markFaultHandled(pidKey string) bool {
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
