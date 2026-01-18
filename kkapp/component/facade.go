package component

type IComponentLifecycle interface {
	Init() error
	AfterInit() error
	BeforeShutdown() error
	Shutdown() error
	GetState() ComponentState
}

type IComponentContainer interface {
	AddChild(child IComponentContainer)
	RemoveChild(child IComponentContainer)
	SetParent(parent IComponentContainer)
	GetParent() IComponentContainer
	GetChildrens() []IComponentContainer
}

type IComponent interface {
	GetID() string
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
	id string
	Lifecycle
	Container
}

var _ IComponent = (*Component)(nil)

func (slf *Component) GetID() string {
	return slf.id
}
