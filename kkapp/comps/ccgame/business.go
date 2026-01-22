package ccgame

import (
	"sync"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp/component"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/kklog"
)

func NewGameComponent() *gameComponent {
	return &gameComponent{}
}

// 业务服：游戏服
type gameComponent struct {
	component.Component
	actorSys    *actor.ActorSystem
	pid         *actor.PID
	responderMu sync.RWMutex
	responder   func(connID kknet.CONN_ID, data []byte)
}

func (slf *gameComponent) GetID() string {
	return "game"
}

var _ component.IComponent = (*gameComponent)(nil)

func (slf *gameComponent) Init() error {
	slf.actorSys = actor.NewActorSystem()
	return nil
}

func (slf *gameComponent) Start() error {
	props := actor.PropsFromProducer(func() actor.Actor {
		return &gameActor{game: slf}
	})
	slf.pid = slf.actorSys.Root.Spawn(props)
	kklog.Infof("[ccgame] game actor started")
	return nil
}

func (slf *gameComponent) Stop() error {
	if slf.pid != nil {
		slf.actorSys.Root.Stop(slf.pid)
		slf.pid = nil
	}
	return nil
}

// HandleRequest forwards a client message to the game actor.
func (slf *gameComponent) HandleRequest(connID kknet.CONN_ID, data []byte) {
	if slf.pid == nil {
		return
	}
	slf.actorSys.Root.Send(slf.pid, &GameRequest{
		ConnID: connID,
		Data:   data,
	})
}

// SetResponder sets the responder for sending data back to gate.
func (slf *gameComponent) SetResponder(responder func(connID kknet.CONN_ID, data []byte)) {
	slf.responderMu.Lock()
	slf.responder = responder
	slf.responderMu.Unlock()
}

func (slf *gameComponent) respond(connID kknet.CONN_ID, data []byte) {
	slf.responderMu.RLock()
	responder := slf.responder
	slf.responderMu.RUnlock()
	if responder != nil {
		responder(connID, data)
	}
}

// GameRequest represents a game request from a client.
type GameRequest struct {
	ConnID kknet.CONN_ID
	Data   []byte
}

type gameActor struct {
	game *gameComponent
}

func (a *gameActor) Receive(ctx actor.Context) {
	switch msg := ctx.Message().(type) {
	case *GameRequest:
		// TODO: add business logic
		kklog.Infof("[ccgame] recv request: connID=%d size=%d", msg.ConnID, len(msg.Data))
		if a.game != nil {
			a.game.respond(msg.ConnID, msg.Data)
		}
	}
}
