package component

import (
	"sync"
	"sync/atomic"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kklog"
)

// each application is a node. each node is a process.
type Application struct {
	nodeInfo *kkapp.NodeInfo
	actorSys *actor.ActorSystem
	pid      *actor.PID
	state    ComponentState
	compList []IComponent
	compPIDs map[string]*actor.PID
	mu       sync.RWMutex

	configDir string // 配置文件所在目录
}

var _ IApplication = (*Application)(nil)

// new application.
// each application is a node, a actor
func NewApplication(nodeInfo *kkapp.NodeInfo) *Application {
	if nodeInfo == nil {
		panic("nodeInfo is nil")
	}
	app := &Application{
		nodeInfo: nodeInfo,
		actorSys: actor.NewActorSystem(),
		state:    ComponentStateNone,
		compList: make([]IComponent, 0),
		compPIDs: make(map[string]*actor.PID),
	}
	return app
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

func (slf *Application) GetActorSystem() *actor.ActorSystem {
	return slf.actorSys
}

func (slf *Application) GetPID() *actor.PID {
	return slf.pid
}

func (slf *Application) GetChildPID(actorName string) *actor.PID {
	slf.mu.RLock()
	pid, ok := slf.compPIDs[actorName]
	slf.mu.RUnlock()
	if !ok {
		return nil
	}
	return pid
}

func (slf *Application) Start() error {
	if atomic.CompareAndSwapInt64(&slf.state, ComponentStateNone, ComponentStateStarting) {
		slf.pid = slf.actorSys.Root.Spawn(actor.PropsFromFunc(slf.Receive))
		return nil
	}
	return kkerrors.ErrAppAlreadyStarted
}

func (slf *Application) Stop() error {
	if slf.pid == nil {
		kklog.Errorf("[kkapp] stop failed. application %s not started", slf.GetNodeId())
		return kkerrors.ErrAppNotStarted
	}
	if !atomic.CompareAndSwapInt64(&slf.state, ComponentStateStarted, ComponentStateStoping) {
		kklog.Errorf("[kkapp] stop failed. application %s not started", slf.GetNodeId())
		return kkerrors.ErrAppNotStarted
	}
	// 等待 Application actor 完全退出，否则进程可能在 Stopping/Stopped 未处理时就退出，看不到日志
	err := slf.actorSys.Root.StopFuture(slf.pid).Wait()
	if err != nil {
		kklog.Errorf("[kkapp] application %s stop error: %v", slf.GetNodeId(), err)
		return err
	}
	atomic.CompareAndSwapInt64(&slf.state, ComponentStateStoping, ComponentStateStoped)
	slf.pid = nil
	return nil
}

// 将comp作为一个子actor添加到application中
//
// 注意:
//
//	-启动顺序和添加顺序相反，先添加的后启动；
//	-停止顺序和启动顺序相反，先启动的后停止；
func (slf *Application) AddComponent(comp IComponent) error {
	if slf.getComponent(comp) != nil {
		kklog.Errorf("[kkapp] application %s repeat add component %s", slf.GetNodeId(), comp.GetCompName())
		return kkerrors.ErrComponentAlreadyAdded
	}
	if atomic.LoadInt64(&slf.state) != ComponentStateNone {
		kklog.Errorf("[kkapp] application %s add component %s failed. not none state", slf.GetNodeId(), comp.GetCompName())
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

func (slf *Application) getComponent(comp IComponent) IComponent {
	slf.mu.RLock()
	defer slf.mu.RUnlock()
	for _, c := range slf.compList {
		if IsEqual(c, comp) {
			return comp
		}
	}
	return nil
}

func (slf *Application) Receive(ctx actor.Context) {
	switch ctx.Message().(type) {
	case *actor.Started:
		if !atomic.CompareAndSwapInt64(&slf.state, ComponentStateStarting, ComponentStateStarted) {
			kklog.Errorf("[kkapp] application %s already started", slf.GetNodeId())
			return
		}
		kklog.Infof("[kkapp] application %s started", slf.GetNodeId())
		slf.mu.RLock()
		comps := make([]IComponent, len(slf.compList))
		copy(comps, slf.compList)
		slf.mu.RUnlock()
		for _, comp := range comps {
			props := actor.PropsFromFunc(comp.Receive)
			pid := ctx.Spawn(props)
			comp.setPID(pid)
			//atomic.CompareAndSwapInt64(&comp.getBase().state, ComponentStateNone, ComponentStateStarting)
			slf.mu.Lock()
			slf.compPIDs[comp.GetCompName()] = pid
			slf.mu.Unlock()
		}
	case *actor.Stopping:
		kklog.Infof("[kkapp] application %s stopping", slf.GetNodeId())
	case *actor.Stopped:
		kklog.Infof("[kkapp] application %s stopped", slf.GetNodeId())
	case *actor.Restarting:
		kklog.Infof("[kkapp] application %s restarting", slf.GetNodeId())
	}
}
