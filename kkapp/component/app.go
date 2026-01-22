package component

import (
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kklog"
)

type INode interface {
	NodeID() string              // 节点id(全局唯一)
	NodeType() string            // 节点类型
	Address() string             // 对外网络监听地址(前端节点用)
	RpcAddress() string          // rpc监听地址(未用)
	Enabled() bool               // 是否启用
	Settings() map[string]string // 节点配置参数
}

type IApplication interface {
	Start() error
	Stop() error
	AddCompenent(child IComponent) error
	HasComponent(child IComponent) bool
	GetComponents() []IComponent
}

type Application struct {
	compList []IComponent
}

func NewApplication() *Application {
	return &Application{}
}

func (slf *Application) GetNodeId() string {
	return "application"
}

func (slf *Application) Start() error {
	compList := slf.compList
	for _, comp := range compList {
		if err := comp.Start(); err != nil {
			kklog.Errorf("[kkapp] application %s start component %s error: %v", slf.GetNodeId(), comp.GetID(), err)
			return err
		}
		kklog.Infof("[kkapp] application %s start component %s success", slf.GetNodeId(), comp.GetID())
	}
	return nil
}

func (slf *Application) Stop() error {
	compList := slf.compList
	for i := len(compList) - 1; i >= 0; i-- {
		if err := compList[i].Stop(); err != nil {
			kklog.Errorf("[kkapp] application %s stop component %s error: %v", slf.GetNodeId(), compList[i].GetID(), err)
		}
		kklog.Infof("[kkapp] application %s stop component %s success", slf.GetNodeId(), compList[i].GetID())
	}
	return nil
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
