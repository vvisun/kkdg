package component

import (
	"github.com/vvisun/kkdg/kkerrors"
)

type IComponentLifecycle interface {
	Init() error //初始化组件
	//OnInit() error  //初始化组件时调用
	Start() error //启动组件
	//OnStart() error //启动组件时调用
	Stop() error //停止组件
	//OnStop() error  //停止组件时调用
}

type IComponentContainer interface {
	AddChild(child IComponent) error
	RemoveChild(child IComponent) error
	GetParent() IComponent
	GetChildrens() []IComponent
	GetRoot() IComponent
}

type IComponent interface {
	GetID() string   // 组件ID。unique id for the component.
	GetName() string // 组件名称。name for the component.
	IComponentLifecycle
	IComponentContainer
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
	name      string
	parent    IComponent
	childlist []IComponent
}

var _ IComponent = (*Component)(nil)

func IsEqual(a, b IComponent) bool {
	return a == b || a.GetID() == b.GetID()
}

func (slf *Component) HasChild(target IComponent) bool {
	for _, child := range slf.childlist {
		if IsEqual(child, target) {
			return true
		}
	}
	return false
}

func (slf *Component) AddChild(child IComponent) error {
	if child.GetParent() != nil {
		return kkerrors.ErrComponentAlreadySetParent
	}
	if slf.HasChild(child) {
		return kkerrors.ErrComponentAlreadyAdded
	}
	slf.childlist = append(slf.childlist, child)
	child.(*Component).parent = slf
	return nil
}

func (slf *Component) RemoveChild(target IComponent) error {
	for i, child := range slf.childlist {
		if IsEqual(child, target) {
			if err := child.Stop(); err != nil {
				return kkerrors.FormatErrorErr("component", err, "remove child failed: %s", child.GetName())
			}

			target.(*Component).parent = nil
			slf.childlist = append(slf.childlist[:i], slf.childlist[i+1:]...)

			break
		}
	}
	return nil
}

func (slf *Component) GetParent() IComponent {
	return slf.parent
}

func (slf *Component) GetChildrens() []IComponent {
	return slf.childlist
}

func (slf *Component) GetRoot() IComponent {
	if slf.parent == nil {
		return slf
	}
	return slf.parent.GetRoot()
}

func (slf *Component) GetID() string {
	return slf.id
}

func (slf *Component) GetName() string {
	if slf.name != "" {
		return slf.name
	}
	name := slf.GetID()
	cur := slf.parent
	for cur != nil {
		name = cur.GetID() + "." + name
		cur = cur.GetParent()
	}
	slf.name = name
	return slf.name
}

// Init was called to initialize the component.
func (slf *Component) Init() error {
	return nil
}

// OnInit was called to initialize the component.
func (slf *Component) OnInit() error {
	return nil
}

// Start was called to start the component.
func (slf *Component) Start() error {
	return nil
}

// OnStart was called to start the component.
func (slf *Component) OnStart() error {
	return nil
}

// Stop was called to stop the component.
func (slf *Component) Stop() error {
	return nil
}

// OnStop was called to stop the component.
func (slf *Component) OnStop() error {
	return nil
}
