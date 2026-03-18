package component

import (
	"sync"
	"sync/atomic"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/kkactor"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kklog"
)

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
//	if actorFramework is nil, will use default getGlobalActorFramework()
func NewApplication(nodeInfo *kkapp.NodeInfo, af *kkactor.ActorFramework, opts kkapp.AppOptions) *Application {
	if nodeInfo == nil {
		// 启动期间的异常装配直接panic，不然反而将隐含问题带到了运行期间，造成不可预测的错误
		kklog.PanicLog("nodeInfo is nil")
	}
	if af == nil {
		kklog.Infof("[kkapp] (nodeId: %s, nodeType: %s) new application actorFramework is nil, use default", nodeInfo.GetNodeId(), nodeInfo.GetNodeType())
		af = getGlobalActorFramework()
	}
	af.GetLocator().AddNode(nodeInfo)
	app := &Application{
		nodeInfo:       nodeInfo,
		actorFramework: af,
		state:          ComponentStateNone,
		compList:       make([]kkapp.IComponent, 0),
		opts:           &opts,
	}
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

	configDir string // 配置文件所在目录
	opts      *kkapp.AppOptions
}

var _ kkapp.IApplication = (*Application)(nil)

func (slf *Application) GetCompName() string {
	return slf.nodeInfo.GetNodeId()
}

func (slf *Application) SetConfigDir(configDir string) {
	slf.configDir = configDir
}

func (slf *Application) GetConfigDir() string {
	return slf.configDir
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

func (slf *Application) GetActorFramework() *kkactor.ActorFramework {
	return slf.actorFramework
}

func (slf *Application) GetPID() *actor.PID {
	return slf.pid
}

func (slf *Application) GetOptions() *kkapp.AppOptions {
	return slf.opts
}

func (slf *Application) GetCompPID(compName string) *actor.PID {
	id, err := kkactor.NewLucencyActorID(slf.GetNodeId(), compName)
	if err != nil {
		kklog.Debugf("[kkapp] application %s get component %s pid error: %v", slf.GetNodeId(), compName, err)
		return nil
	}
	pid, err := slf.actorFramework.GetLocator().GetActor(id)
	if err != nil {
		kklog.Debugf("[kkapp] application %s get component %s pid error: %v", slf.GetNodeId(), compName, err)
		return nil
	}
	return pid
}

func (slf *Application) Start() error {
	if !atomic.CompareAndSwapInt64(&slf.state, ComponentStateNone, ComponentStateStarting) {
		kklog.Errorf("[kkapp] application %s start fail. already started, state: %s",
			slf.GetNodeId(), GetStateName(ComponentState(atomic.LoadInt64(&slf.state))))
		return kkerrors.ErrAppAlreadyStarted
	}
	startResultCh := make(chan error, 1)
	slf.mu.Lock()
	slf.startResultCh = startResultCh
	slf.mu.Unlock()
	kklog.Infof("[kkapp] application %s starting", slf.GetNodeId())
	slf.pid = slf.actorFramework.GetActorSystem().Root.Spawn(actor.PropsFromFunc(slf.Receive))
	if slf.pid == nil {
		kklog.Errorf("[kkapp] application %s spawn actor fail", slf.GetNodeId())
		// 启动期间的异常装配直接panic，不然反而将隐含问题带到了运行期间，造成不可预测的错误
		kklog.PanicErr(kkerrors.ErrAppSpawnActorFailed)
	}
	id, err := kkactor.NewLucencyActorID(slf.GetNodeId(), slf.GetCompName())
	if err != nil {
		kklog.Errorf("[kkapp] application %s add component %s error: %v", slf.GetNodeId(), slf.GetCompName(), err)
		// 启动期间的异常装配直接panic，不然反而将隐含问题带到了运行期间，造成不可预测的错误
		kklog.PanicErr(err)
	}
	err = slf.actorFramework.GetLocator().AddActor(id, slf.pid)
	if err != nil {
		kklog.Errorf("[kkapp] application %s add component %s error: %v", slf.GetNodeId(), slf.GetCompName(), err)
		// 启动期间的异常装配直接panic，不然反而将隐含问题带到了运行期间，造成不可预测的错误
		kklog.PanicErr(err)
	}
	return <-startResultCh
}

func (slf *Application) Stop() error {
	if slf.pid == nil {
		kklog.Errorf("[kkapp] stop fail. application %s not started, state: %s",
			slf.GetNodeId(), GetStateName(ComponentState(atomic.LoadInt64(&slf.state))))
		return kkerrors.ErrAppNotStarted
	}
	if !atomic.CompareAndSwapInt64(&slf.state, ComponentStateStarted, ComponentStateStopping) {
		kklog.Errorf("[kkapp] stop fail. application %s not started, state: %s",
			slf.GetNodeId(), GetStateName(ComponentState(atomic.LoadInt64(&slf.state))))
		return kkerrors.ErrAppNotStarted
	}
	// 等待 Application actor 完全退出，否则进程可能在 Stopping/Stopped 未处理时就退出，看不到日志
	err := slf.actorFramework.GetActorSystem().Root.PoisonFuture(slf.pid).Wait()
	if err != nil {
		kklog.Errorf("[kkapp] application %s stop error: %v", slf.GetNodeId(), err)
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
//	-启动顺序和添加顺序相反，先添加的后启动；
//	-停止顺序和启动顺序相反，先启动的后停止；
func (slf *Application) AddComponent(comp kkapp.IComponent) error {
	if _, err := kkactor.NewLucencyActorID(slf.GetNodeId(), comp.GetCompName()); err != nil {
		kklog.Errorf("[kkapp] application %s add component %s error: %v", slf.GetNodeId(), comp.GetCompName(), err)
		return err
	}
	if slf.existsComponent(comp) {
		kklog.Errorf("[kkapp] application %s already has component %s",
			slf.GetNodeId(), comp.GetCompName())
		return kkerrors.ErrComponentAlreadyAdded
	}
	if atomic.LoadInt64(&slf.state) != ComponentStateNone {
		kklog.Errorf("[kkapp] application %s add component %s fail. not none state, state: %s",
			slf.GetNodeId(), comp.GetCompName(), GetStateName(ComponentState(atomic.LoadInt64(&slf.state))))
		return kkerrors.ErrAppAddCompMustInNoneState
	}

	comp.SetApplication(slf)
	err := comp.OnInit()
	if err != nil {
		kklog.Errorf("[kkapp] application %s init component %s error: %v", slf.GetNodeId(), comp.GetCompName(), err)
		return err
	}

	slf.mu.Lock()
	slf.compList = append(slf.compList, comp)
	slf.mu.Unlock()
	kklog.Infof("[kkapp] application %s add component %s", slf.GetNodeId(), comp.GetCompName())
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
		kklog.Errorf("[kkapp] application %s already started, state: %s",
			slf.GetNodeId(), GetStateName(ComponentState(atomic.LoadInt64(&slf.state))))
		return //已经启动，直接返回
	}

	slf.mu.RLock()
	comps := make([]kkapp.IComponent, len(slf.compList))
	copy(comps, slf.compList)
	slf.mu.RUnlock()

	for _, comp := range comps {
		props := actor.PropsFromFunc(comp.Receive)
		pid := ctx.Spawn(props)
		if pid == nil {
			kklog.Errorf("[kkapp] application %s spawn component %s fail", slf.GetNodeId(), comp.GetCompName())
			// 启动期间的异常装配直接panic，不然反而将隐含问题带到了运行期间，造成不可预测的错误
			kklog.PanicErr(kkerrors.ErrAppSpawnActorFailed)
		}

		comp.SetPID(pid)

		id, err := kkactor.NewLucencyActorID(slf.GetNodeId(), comp.GetCompName())
		if err != nil {
			kklog.Errorf("[kkapp] application %s add component %s error: %v", slf.GetNodeId(), comp.GetCompName(), err)
			// 启动期间的异常装配直接panic，不然反而将隐含问题带到了运行期间，造成不可预测的错误
			kklog.PanicErr(err)
		}
		err = slf.actorFramework.GetLocator().AddActor(id, pid)
		if err != nil {
			kklog.Errorf("[kkapp] application %s add component %s error: %v", slf.GetNodeId(), comp.GetCompName(), err)
			// 启动期间的异常装配直接panic，不然反而将隐含问题带到了运行期间，造成不可预测的错误
			kklog.PanicErr(err)
		}
		if err := comp.OnStart(); err != nil {
			kklog.Errorf("[kkapp] application %s start component %s error: %v", slf.GetNodeId(), comp.GetCompName(), err)
			atomic.CompareAndSwapInt64(&slf.state, ComponentStateStarting, ComponentStateStopping)
			slf.finishStart(err)
			ctx.Stop(ctx.Self())
			return
		}
	}
	atomic.CompareAndSwapInt64(&slf.state, ComponentStateStarting, ComponentStateStarted)
	kklog.Infof("[kkapp] application %s started", slf.GetNodeId())
	slf.finishStart(nil)
}

func (slf *Application) onStopped() {
	atomic.CompareAndSwapInt64(&slf.state, ComponentStateStopping, ComponentStateStopped)
	id, _ := kkactor.NewLucencyActorID(slf.GetNodeId(), slf.GetCompName())
	slf.actorFramework.GetLocator().RemoveActor(id)
	slf.actorFramework.GetLocator().RemoveNode(slf.nodeInfo)
	slf.mu.Lock()
	slf.compList = make([]kkapp.IComponent, 0)
	slf.mu.Unlock()
	kklog.Infof("[kkapp] application %s stopped", slf.GetNodeId())
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

func (slf *Application) Receive(ctx actor.Context) {
	switch ctx.Message().(type) {
	case *actor.Started:
		slf.onStarted(ctx)
	case *actor.Stopping:
		kklog.Infof("[kkapp] application %s stopping", slf.GetNodeId())
	case *actor.Stopped:
		slf.onStopped()
	case *actor.Restarting:
		kklog.Infof("[kkapp] application %s restarting", slf.GetNodeId())
	}
}
