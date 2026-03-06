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
type IApplication interface {
	kkapp.INodeIdentity
	GetNodeInfo() *kkapp.NodeInfo
	Start() error
	Stop() error
	GetActorSystem() *actor.ActorSystem

	AddComponent(child IComponent) error
	HasComponent(child IComponent) bool
	GetComponents() []IComponent

	SetConfigDir(configDir string)
	GetConfigDir() string
}

// each application is a node. each node is a process.
type Application struct {
	nodeInfo  *kkapp.NodeInfo
	actorSys  *actor.ActorSystem
	compList  []IComponent
	mu        sync.RWMutex
	stoping   atomic.Bool
	configDir string // 配置文件所在目录
}

var _ IApplication = (*Application)(nil)

// new application.
// each application is a node. each node is a process.
func NewApplication(nodeInfo *kkapp.NodeInfo) *Application {
	if nodeInfo == nil {
		panic("nodeInfo is nil")
	}
	return &Application{
		nodeInfo: nodeInfo,
		actorSys: actor.NewActorSystem(),
		compList: make([]IComponent, 0),
	}
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

func (slf *Application) Start() error {
	nodeId := slf.nodeInfo.GetNodeId()
	kklog.Infof("[kkapp] application [%s,%s] starting", nodeId, slf.nodeInfo.GetNodeType())
	slf.mu.RLock()
	compList := slf.compList
	slf.mu.RUnlock()
	for _, comp := range compList {
		if err := comp.Start(); err != nil {
			kklog.Errorf("[kkapp] application %s start component %s error: %v", nodeId, comp.GetID(), err)
			return err
		}
		kklog.Infof("[kkapp] application %s start component %s success", nodeId, comp.GetID())
	}
	kklog.Infof("[kkapp] application [%s,%s] started", nodeId, slf.nodeInfo.GetNodeType())
	return nil
}

func (slf *Application) Stop() error {
	if !slf.stoping.CompareAndSwap(false, true) {
		return nil // already stopping
	}
	nodeId := slf.nodeInfo.GetNodeId()
	kklog.Infof("[kkapp] application [%s,%s] stopping", nodeId, slf.nodeInfo.GetNodeType())
	slf.mu.RLock()
	compList := slf.compList
	slf.mu.RUnlock()
	for i := len(compList) - 1; i >= 0; i-- {
		if err := compList[i].Stop(); err != nil {
			kklog.Errorf("[kkapp] application %s stop component %s error: %v", nodeId, compList[i].GetID(), err)
		}
		kklog.Infof("[kkapp] application %s stop component %s success", nodeId, compList[i].GetID())
	}
	kklog.Infof("[kkapp] application [%s,%s] stopped", nodeId, slf.nodeInfo.GetNodeType())
	return nil
}

func (slf *Application) AddComponent(comp IComponent) error {
	if slf.stoping.Load() {
		kklog.Errorf("[kkapp] application %s add component %s error: %v", slf.GetNodeId(), comp.GetID(), kkerrors.ErrAppShutdown)
		return kkerrors.ErrAppShutdown
	}
	if slf.HasComponent(comp) {
		kklog.Errorf("[kkapp] application %s add component %s repeat: %v", slf.GetNodeId(), comp.GetID(), kkerrors.ErrComponentAlreadyAdded)
		return kkerrors.ErrComponentAlreadyAdded
	}
	comp.SetApplication(slf)

	// Initialize the component before adding it to the list, so a failed init
	// won't leave a half-added component inside the application.
	if err := comp.Init(); err != nil {
		kklog.Errorf("[kkapp] application %s init component %s error: %v", slf.GetNodeId(), comp.GetID(), err)
		return err
	}

	slf.mu.Lock()
	slf.compList = append(slf.compList, comp)
	slf.mu.Unlock()
	return nil
}

func (slf *Application) HasComponent(comp IComponent) bool {
	slf.mu.RLock()
	defer slf.mu.RUnlock()
	for _, c := range slf.compList {
		if IsEqual(c, comp) {
			return true
		}
	}
	return false
}

func (slf *Application) GetComponents() []IComponent {
	slf.mu.RLock()
	defer slf.mu.RUnlock()
	return slf.compList
}
