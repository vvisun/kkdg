package component

import (
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/xreflect"
)

type IComponentLifecycle interface {
	Init() error  //初始化组件
	Start() error //启动组件
	Stop() error  //停止组件
}

type IComponentContainer interface {
	AddComponent(child IComponent) error
	RemoveComponent(child IComponent) error
	GetComponents() []IComponent
}

type IComponent interface {
	GetID() string // 组件ID。unique id for the component.
	IComponentLifecycle
	IComponentContainer
	SetApplication(app IApplication)
	GetApplication() IApplication
}

func IsEqual(a, b IComponent) bool {
	return a == b || a.GetID() == b.GetID()
}

func GetComponentName(comp IComponent) string {
	return xreflect.GetStructName(comp) + "_" + comp.GetID()
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
	id        string
	app       IApplication
	childlist []IComponent
}

var _ IComponent = (*Component)(nil)

func (slf *Component) GetID() string {
	return slf.id
}

func (slf *Component) SetApplication(app IApplication) {
	slf.app = app
}

func (slf *Component) GetApplication() IApplication {
	return slf.app
}

func (slf *Component) HasComponent(target IComponent) bool {
	for _, child := range slf.childlist {
		if IsEqual(child, target) {
			return true
		}
	}
	return false
}

func (slf *Component) AddComponent(target IComponent) error {
	if slf.HasComponent(target) {
		return kkerrors.ErrComponentAlreadyAdded
	}
	slf.childlist = append(slf.childlist, target)
	return nil
}

func (slf *Component) RemoveComponent(target IComponent) error {
	for i, child := range slf.childlist {
		if IsEqual(child, target) {
			if err := child.Stop(); err != nil {
				return kkerrors.FormatErrorErr("component", err, "remove child failed: %s", child.GetID())
			}
			slf.childlist = append(slf.childlist[:i], slf.childlist[i+1:]...)
			break
		}
	}
	return nil
}

func (slf *Component) GetComponents() []IComponent {
	return slf.childlist
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
