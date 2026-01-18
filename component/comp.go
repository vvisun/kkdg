package component

import "errors"

type IComponent interface {
	Init() error
	AfterInit() error
	BeforeShutdown() error
	Shutdown() error
	AddChild(child IComponent)
	RemoveChild(child IComponent)
	SetParent(parent IComponent)
	GetParent() IComponent
	GetChildrens() []IComponent
	GetState() ComponentState
}

type ComponentState int

const (
	ComponentStateNone           ComponentState = iota //组件未初始化
	ComponentStateInit                                 //组件初始化中
	ComponentStateAfterInit                            //组件初始化后
	ComponentStateBeforeShutdown                       //组件关闭前
	ComponentStateShutdown                             //组件已关闭
)

type Component struct {
	state     ComponentState
	parent    IComponent
	childlist []IComponent
}

func (c *Component) GetState() ComponentState {
	return c.state
}

func (c *Component) AddChild(child IComponent) {
	c.childlist = append(c.childlist, child)
	child.SetParent(c)
}

func (c *Component) RemoveChild(child IComponent) {
	for i, child := range c.childlist {
		if child == child {
			c.childlist = append(c.childlist[:i], c.childlist[i+1:]...)
			child.SetParent(nil)
			break
		}
	}
}

func (c *Component) SetParent(parent IComponent) {
	c.parent = parent
}

func (c *Component) GetParent() IComponent {
	return c.parent
}

func (c *Component) GetChildrens() []IComponent {
	return c.childlist
}

func (c *Component) Init() error {
	if c.state != ComponentStateNone {
		return errors.New("component state is not none")
	}
	c.state = ComponentStateInit
	return nil
}

func (c *Component) AfterInit() error {
	if c.state != ComponentStateInit {
		return errors.New("component state is not init")
	}
	c.state = ComponentStateAfterInit
	return nil
}

func (c *Component) BeforeShutdown() error {
	if c.state != ComponentStateAfterInit {
		return errors.New("component state is not after init")
	}
	c.state = ComponentStateBeforeShutdown
	return nil
}

func (c *Component) Shutdown() error {
	if c.state != ComponentStateBeforeShutdown {
		return errors.New("component state is not before shutdown")
	}
	c.state = ComponentStateShutdown
	return nil
}
