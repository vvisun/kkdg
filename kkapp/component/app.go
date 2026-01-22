package component

import (
	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kklog"
)

type IApplication interface {
	GetNodeInfo() *kkapp.NodeInfo
	Start() error
	Stop() error      // 立即停止
	GraceStop() error // 优雅停止

	AddCompenent(child IComponent) error
	HasComponent(child IComponent) bool
	GetComponents() []IComponent
}

type Application struct {
	nodeInfo *kkapp.NodeInfo
	compList []IComponent
}

var _ IApplication = (*Application)(nil)

func NewApplication(nodeInfo *kkapp.NodeInfo) *Application {
	return &Application{
		nodeInfo: nodeInfo,
		compList: make([]IComponent, 0),
	}
}

func (slf *Application) GetNodeInfo() *kkapp.NodeInfo {
	return slf.nodeInfo
}

func (slf *Application) Start() error {
	nodeId := slf.nodeInfo.GetNodeId()
	compList := slf.compList
	for _, comp := range compList {
		if err := comp.Start(); err != nil {
			kklog.Errorf("[kkapp] application %s start component %s error: %v", nodeId, comp.GetID(), err)
			return err
		}
		kklog.Infof("[kkapp] application %s start component %s success", nodeId, comp.GetID())
	}
	return nil
}

func (slf *Application) Stop() error {
	nodeId := slf.nodeInfo.GetNodeId()
	compList := slf.compList
	for i := len(compList) - 1; i >= 0; i-- {
		if err := compList[i].Stop(); err != nil {
			kklog.Errorf("[kkapp] application %s stop component %s error: %v", nodeId, compList[i].GetID(), err)
		}
		kklog.Infof("[kkapp] application %s stop component %s success", nodeId, compList[i].GetID())
	}
	return nil
}

func (slf *Application) GraceStop() error {
	return slf.Stop()
}

func (slf *Application) AddCompenent(comp IComponent) error {
	if slf.HasComponent(comp) {
		return kkerrors.ErrComponentAlreadyAdded
	}
	comp.SetApplication(slf)
	slf.compList = append(slf.compList, comp)
	return nil
}

func (slf *Application) HasComponent(comp IComponent) bool {
	for _, c := range slf.compList {
		if IsEqual(c, comp) {
			return true
		}
	}
	return false
}

func (slf *Application) GetComponents() []IComponent {
	return slf.compList
}
