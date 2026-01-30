package ccgame

import (
	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp/component"
)

func NewGameComponent() *gameComponent {
	return &gameComponent{}
}

// 业务服：游戏服
type gameComponent struct {
	component.Component
	pid *actor.PID
}

func (slf *gameComponent) GetID() string {
	return "game"
}

var _ component.IComponent = (*gameComponent)(nil)

func (slf *gameComponent) Init() error {
	return nil
}

func (slf *gameComponent) Start() error {
	return nil
}

func (slf *gameComponent) Stop() error {
	if slf.pid != nil {
		slf.GetApplication().GetActorSystem().Root.Stop(slf.pid)
		slf.pid = nil
	}
	return nil
}
