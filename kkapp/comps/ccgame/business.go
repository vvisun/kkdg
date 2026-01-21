package ccgame

import "github.com/vvisun/kkdg/kkapp/component"

//业务服：游戏服
type GameComponent struct {
	component.Component
}

func (slf *GameComponent) GetID() string {
	return "game"
}

var _ component.IComponent = (*GameComponent)(nil)

func (slf *GameComponent) Init() error {
	return nil
}
