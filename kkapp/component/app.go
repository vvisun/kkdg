package component

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/faultreport"
	"github.com/vvisun/kkdg/kkapp/kkactor"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kkevent"
	"github.com/vvisun/kkdg/utils/kklog"
)

// new application.
//
//	each application is a node, a actor.
func NewApplication(nodeInfo *kkapp.NodeInfo, af *kkactor.ActorFramework, opts kkapp.AppOptions) *Application {
	if nodeInfo == nil {
		// 启动期间的异常装配直接panic，不然反而将隐含问题带到了运行期间，造成不可预测的错误
		kklog.PanicLog("nodeInfo is nil")
	}
	if af == nil {
		// 启动期间的异常装配直接panic，不然反而将隐含问题带到了运行期间，造成不可预测的错误
		kklog.PanicLog("actorFramework is nil")
	}
	kkapp.CheckOptions(&opts)
	_ = af.GetLocalActorMgr().AddLocalNode(nodeInfo.GetNodeId())
	app := &Application{
		nodeInfo:       nodeInfo,
		actorFramework: af,
		state:          ComponentStateNone,
		compList:       make([]kkapp.IComponent, 0),
		opts:           &opts,
	}
	app.watcher = newAppWatcher(app)
	app.watcher.faultRuleTable.FromMap(app.opts.FaultRuleMap)
	app.watcher.faultRuleTable.Lock()
	kklog.Infof("[kkapp] (nodeId: %s, nodeType: %s) new application", nodeInfo.GetNodeId(), nodeInfo.GetNodeType())
	return app
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

	watcher *appWatcher
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
	return slf.watcher.faultEventMgr
}

func (slf *Application) logTag() string {
	return fmt.Sprintf("[kkapp] (nodeId: %s, nodeType: %s)", slf.GetNodeId(), slf.GetNodeType())
}

func (slf *Application) curStateName() string {
	return GetStateName(atomic.LoadInt64(&slf.state))
}

func (slf *Application) GetCompPID(compName string) *actor.PID {
	id, err := kkactor.NewLucencyID(slf.GetNodeId(), compName)
	if err != nil {
		kklog.Debugf("%s get component %s pid error: %v", slf.logTag(), compName, err)
		return nil
	}
	pid, err := slf.actorFramework.GetLocalActorMgr().GetActor(id)
	if err != nil {
		kklog.Debugf("%s get component %s pid error: %v", slf.logTag(), compName, err)
		return nil
	}
	return pid
}

func (slf *Application) Start() error {
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
	sup := slf.watcher.supervisorStrategy()
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
	id, err := kkactor.NewLucencyID(slf.GetNodeId(), slf.GetCompName())
	if err != nil {
		kklog.Errorf("%s add component %s error: %v", slf.logTag(), slf.GetCompName(), err)
		// 启动期间的异常装配直接panic，不然反而将隐含问题带到了运行期间，造成不可预测的错误
		kklog.PanicErr(err)
	}
	err = slf.actorFramework.GetLocalActorMgr().AddActor(id, slf.pid)
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
	if _, err := kkactor.NewLucencyID(slf.GetNodeId(), comp.GetCompName()); err != nil {
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

	slf.watcher.initSupervisorEvent(ctx)

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

		slf.watcher.watchComponent(ctx, pid, comp.GetCompName())

		id, err := kkactor.NewLucencyID(slf.GetNodeId(), comp.GetCompName())
		if err != nil {
			kklog.Errorf("%s add component %s error: %v", slf.logTag(), comp.GetCompName(), err)
			// 启动期间的异常装配直接panic，不然反而将隐含问题带到了运行期间，造成不可预测的错误
			kklog.PanicErr(err)
		}
		err = slf.actorFramework.GetLocalActorMgr().AddActor(id, pid)
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

	slf.watcher.onStopped()

	id, _ := kkactor.NewLucencyID(slf.GetNodeId(), slf.GetCompName())
	slf.actorFramework.GetLocalActorMgr().RemoveActor(id)
	_ = slf.actorFramework.GetLocalActorMgr().RemoveLocalNode(slf.GetNodeId())
	slf.mu.Lock()
	slf.compList = make([]kkapp.IComponent, 0)
	slf.mu.Unlock()

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

func (slf *Application) onTerminated(ctx actor.Context) {
	curState := atomic.LoadInt64(&slf.state)
	if curState == ComponentStateStopping || curState == ComponentStateStopped {
		// Normal shutdown path: ignore component termination while application is stopping/stopped.
		return
	}
	slf.watcher.onTerminated(ctx)
}
