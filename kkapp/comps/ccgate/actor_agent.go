package ccgate

import (
	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kknet/kkactor"
)

// ActorAgent 每个网络连接对应一个ActorAgent
type ActorAgent struct {
}

var _ kkactor.Actor = (*ActorAgent)(nil)

func (a *ActorAgent) Receive(ctx actor.Context) {

}
