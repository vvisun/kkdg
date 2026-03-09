package component

import (
	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/utils/xreflect"
)

func IsEqual(a, b IComponent) bool {
	if a == nil || b == nil {
		return false
	}
	return a == b || a.GetCompName() == b.GetCompName()
}

func GetComponentName(comp IComponent) string {
	return xreflect.GetStructName(comp) + "_" + comp.GetCompName()
}

type ComponentState = int64

const (
	ComponentStateNone           ComponentState = iota //组件未初始化
	ComponentStateInit                                 //组件初始化中
	ComponentStateAfterInit                            //组件初始化后
	ComponentStateBeforeShutdown                       //组件关闭前
	ComponentStateShutdown                             //组件已关闭
)

type Component struct {
	id  string
	app IApplication
}

var _ IComponent = (*Component)(nil)

func (slf *Component) GetCompName() string {
	return slf.id
}

func (slf *Component) SetApplication(app IApplication) {
	slf.app = app
}

func (slf *Component) GetApplication() IApplication {
	return slf.app
}

// Init was called to initialize the component.
func (slf *Component) Init() error {
	return nil
}

// Start was called to start the component.
func (slf *Component) Start() error {
	return nil
}

// Stop was called to stop the component.
func (slf *Component) Stop() error {
	return nil
}

func (slf *Component) Equal(other IComponent) bool {
	return IsEqual(slf, other)
}

var _ actor.Actor = (*Component)(nil)

// implement actor.Actor
//
//	每个组件视为1个actor。这样，我们可以做到：
//	组件挂接到任意节点上时，都能实现透明化。
//	单机部署，集群部署都无需修改逻辑。
//	调整组件所属节点时，也无需修改逻辑。
func (slf *Component) Receive(context actor.Context) {

}
