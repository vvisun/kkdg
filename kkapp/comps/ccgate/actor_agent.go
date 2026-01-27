package ccgate

import (
	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/kklog"
)

// ActorAgent 每个网络连接对应一个ActorAgent
type ActorAgent struct {
	connID          kknet.CONN_ID
	conn            kknet.IConn
	router          *Router
	businessHandler IBusinessHandler
}

var _ actor.Actor = (*ActorAgent)(nil)

// NewActorAgent creates a new actor agent.
func NewActorAgent(connID kknet.CONN_ID, conn kknet.IConn, router *Router, businessHandler IBusinessHandler) *ActorAgent {
	return &ActorAgent{
		connID:          connID,
		conn:            conn,
		router:          router,
		businessHandler: businessHandler,
	}
}

func (a *ActorAgent) Receive(ctx actor.Context) {
	switch msg := ctx.Message().(type) {
	case *MessageFromClient:
		// 客户端消息，转发到业务服
		a.handleClientMessage(ctx, msg)
	case *MessageFromBusiness:
		// 业务服消息，转发到客户端
		a.handleBusinessMessage(ctx, msg)
	case *actor.Stopped:
		// Actor 停止，清理连接
		a.router.RemovePID(a.connID)
		if a.conn != nil {
			_ = a.conn.Close()
		}
	}
}

// handleClientMessage 处理来自客户端的消息
func (a *ActorAgent) handleClientMessage(ctx actor.Context, msg *MessageFromClient) {
	if a.businessHandler == nil {
		kklog.Warnf("[ccgate] business handler not found: connID=%d", a.connID)
		return
	}
	a.businessHandler.HandleRequest(a.connID, msg.Data)
	_ = ctx
}

// handleBusinessMessage 处理来自业务服的消息
func (a *ActorAgent) handleBusinessMessage(ctx actor.Context, msg *MessageFromBusiness) {
	if a.conn == nil {
		return
	}
	// 转发到客户端
	_ = a.conn.Send(msg.Data)
	_ = ctx
}

// MessageFromClient 来自客户端的消息
type MessageFromClient struct {
	Data []byte
}

// MessageFromBusiness 来自业务服的消息
type MessageFromBusiness struct {
	ConnID kknet.CONN_ID
	Data   []byte
}
