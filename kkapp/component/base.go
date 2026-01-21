package component

import (
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kklog"
)

type IComponentLifecycle interface {
	Start() error
	BeforeShutdown() error
	Stop() error
	AfterShutdown() error
}

type IComponentContainer interface {
	AddChild(child IComponent, start bool) error
	RemoveChild(child IComponent) error
	GetParent() IComponent
	GetChildrens() []IComponent
	GetRoot() IComponent
}

type IComponent interface {
	GetID() string
	GetName() string
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

func (slf *Component) AddChild(child IComponent, start bool) error {
	if child.GetParent() != nil {
		return kkerrors.ErrComponentAlreadySetParent
	}
	if slf.HasChild(child) {
		return kkerrors.ErrComponentAlreadyAdded
	}
	slf.childlist = append(slf.childlist, child)
	child.(*Component).parent = slf
	if start {
		if err := child.Start(); err != nil {
			return kkerrors.FormatErrorErr("component", err, "add child failed: %s", child.GetID())
		}
	}
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
	name := slf.GetID()
	cur := slf.parent
	for cur != nil {
		name = cur.GetID() + "." + name
		cur = cur.GetParent()
	}
	return name
}

// Start was called to start the component.
func (slf *Component) Start() error {
	return nil
}

// BeforeShutdown was called before the component to shutdown.
func (slf *Component) BeforeShutdown() error {
	for i := len(slf.childlist) - 1; i >= 0; i-- {
		if err := slf.childlist[i].BeforeShutdown(); err != nil {
			kklog.Errorf("[component] %s before shutdown error: %v", slf.childlist[i].GetName(), err)
			return kkerrors.FormatErrorErr("component", err, "before shutdown failed: %s", slf.childlist[i].GetName())
		}
		kklog.Infof("[component] %s before shutdown success", slf.childlist[i].GetName())
	}
	return nil
}

// Stop was called to stop the component.
func (slf *Component) Stop() error {
	for i := len(slf.childlist) - 1; i >= 0; i-- {
		if err := slf.childlist[i].Stop(); err != nil {
			kklog.Errorf("[component] %s stop error: %v", slf.childlist[i].GetName(), err)
		}
		kklog.Infof("[component] %s stop success", slf.childlist[i].GetName())
	}
	return nil
}

// AfterShutdown was called after the component is shutdown.
func (slf *Component) AfterShutdown() error {
	for i := len(slf.childlist) - 1; i >= 0; i-- {
		if err := slf.childlist[i].AfterShutdown(); err != nil {
			kklog.Errorf("[component] %s after shutdown error: %v", slf.childlist[i].GetName(), err)
		}
		kklog.Infof("[component] %s after shutdown success", slf.childlist[i].GetName())
	}
	return nil
}
