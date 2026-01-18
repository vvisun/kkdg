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

type ComponentState int

const (
	ComponentStateNone           ComponentState = iota //组件未初始化
	ComponentStateInit                                 //组件初始化中
	ComponentStateAfterInit                            //组件初始化后
	ComponentStateBeforeShutdown                       //组件关闭前
	ComponentStateShutdown                             //组件已关闭
)

type IComponent interface {
	IComponentLifecycle
	IComponentContainer
}

type Component struct {
	Lifecycle
	Container
}
