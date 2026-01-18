package component

type IComponent interface {
	Init() error
	AfterInit() error
	BeforeShutdown() error
	Shutdown() error
}
