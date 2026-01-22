package ccgate

import (
	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp/component"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/kknet/kkws"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/kklog"
)

// IBusinessHandler 业务处理器接口，用于处理来自客户端的业务请求
type IBusinessHandler interface {
	// HandleRequest 处理客户端请求
	HandleRequest(connID kknet.CONN_ID, data []byte)
	// SetResponder 设置响应回调，用于向客户端发送响应
	SetResponder(responder func(connID kknet.CONN_ID, data []byte))
}

// 网关服
type gateComponent struct {
	component.Component
	opt             Option
	router          *Router
	actorSys        *actor.ActorSystem
	businessHandler IBusinessHandler
	tcpServer       kknet.IServer
	wsServer        kknet.IServer
	handler         *gateHandler
}

func (slf *gateComponent) GetID() string {
	return "gate"
}

var _ component.IComponent = (*gateComponent)(nil)

// NewGateComponent creates a new gate component.
func NewGateComponent(opt Option) *gateComponent {
	return &gateComponent{
		opt: opt,
	}
}

func (slf *gateComponent) Init() error {
	// 初始化 actor system
	slf.actorSys = actor.NewActorSystem()

	// 创建 handler
	slf.handler = newGateHandler(slf)

	// 初始化 router（稍后在服务器启动后添加 connMgr）
	slf.router = NewRouter()

	return nil
}

func (slf *gateComponent) Start() error {
	// 启动 TCP 服务器
	if slf.opt.TCPAddr != "" {
		if err := slf.startTCPServer(); err != nil {
			return err
		}
	}

	// 启动 WebSocket 服务器
	if slf.opt.WSAddr != "" {
		if err := slf.startWSServer(); err != nil {
			return err
		}
	}

	// 设置业务处理器
	if slf.opt.BusinessHandler != nil {
		slf.businessHandler = slf.opt.BusinessHandler
		slf.businessHandler.SetResponder(slf.sendToClient)
	}

	return nil
}

func (slf *gateComponent) Stop() error {
	// 停止 TCP 服务器
	if slf.tcpServer != nil {
		if err := slf.tcpServer.Stop(); err != nil {
			kklog.Errorf("[ccgate] stop tcp server error: %v", err)
		}
	}

	// 停止 WebSocket 服务器
	if slf.wsServer != nil {
		if err := slf.wsServer.Stop(); err != nil {
			kklog.Errorf("[ccgate] stop ws server error: %v", err)
		}
	}

	// 停止所有连接的 actor
	if slf.router != nil {
		connMgr := slf.router.GetConnManager()
		if connMgr != nil {
			allConns := connMgr.GetAllConns()
			for connID := range allConns {
				if pid, ok := slf.router.GetPID(connID); ok {
					slf.actorSys.Root.Stop(pid)
				}
			}
		}
	}

	if slf.businessHandler != nil {
		slf.businessHandler.SetResponder(nil)
	}

	return nil
}

func (slf *gateComponent) startTCPServer() error {
	// 创建 TCP 服务器
	server := kktcp.NewServer(
		slf.opt.TCPAddr,
		slf.handler,
		kknet.WithLogger(kklog.Stdout()),
		kknet.WithStreamPacket(kkpacket.NewLengthFieldStreamPacket(nil)),
	)

	if err := server.Start(); err != nil {
		return err
	}

	slf.tcpServer = server
	// 添加 TCP 服务器的 connMgr 到 router
	slf.router.AddConnManager(server.GetConnManager())

	kklog.Infof("[ccgate] tcp server started on %s", slf.opt.TCPAddr)
	return nil
}

func (slf *gateComponent) startWSServer() error {
	// 创建 WebSocket 服务器
	server := kkws.NewServer(
		slf.opt.WSAddr,
		slf.handler,
		kknet.WithLogger(kklog.Stdout()),
	)

	if err := server.Start(); err != nil {
		return err
	}

	slf.wsServer = server
	// 添加 WebSocket 服务器的 connMgr 到 router
	slf.router.AddConnManager(server.GetConnManager())

	kklog.Infof("[ccgate] ws server started on %s", slf.opt.WSAddr)
	return nil
}

// SetBusinessHandler 设置业务处理器（可选，也可以通过 Option 设置）
func (slf *gateComponent) SetBusinessHandler(handler IBusinessHandler) {
	slf.businessHandler = handler
	if handler != nil {
		handler.SetResponder(slf.sendToClient)
	}
}

func (slf *gateComponent) sendToClient(connID kknet.CONN_ID, data []byte) {
	if slf.router == nil {
		return
	}
	pid, ok := slf.router.GetPID(connID)
	if !ok || pid == nil {
		return
	}
	slf.actorSys.Root.Send(pid, &MessageFromBusiness{
		ConnID: connID,
		Data:   data,
	})
}

// GetRouter returns the router.
func (slf *gateComponent) GetRouter() *Router {
	return slf.router
}

// GetActorSystem returns the actor system.
func (slf *gateComponent) GetActorSystem() *actor.ActorSystem {
	return slf.actorSys
}

// gateHandler 实现 kknet.IHandler
type gateHandler struct {
	gate *gateComponent
}

func newGateHandler(gate *gateComponent) *gateHandler {
	return &gateHandler{
		gate: gate,
	}
}

func (h *gateHandler) OnConnect(c kknet.IConn) {
	// 为每个连接创建一个 ActorAgent
	agent := NewActorAgent(c.ID(), c, h.gate.router, h.gate.businessHandler)
	props := actor.PropsFromProducer(func() actor.Actor { return agent })
	pid := h.gate.actorSys.Root.Spawn(props)

	// 注册 PID
	h.gate.router.AddPID(c.ID(), pid)

	kklog.Infof("[ccgate] client connected: connID=%d, remoteAddr=%s", c.ID(), c.RemoteAddr())
}

func (h *gateHandler) OnMessage(c kknet.IConn, data buffers.IBuffer) {
	// 获取连接对应的 PID
	pid, ok := h.gate.router.GetPID(c.ID())
	if !ok {
		kklog.Warnf("[ccgate] connection not found: connID=%d", c.ID())
		return
	}

	// 发送消息到对应的 Actor
	msg := &MessageFromClient{
		Data: data.Bytes(),
	}
	h.gate.actorSys.Root.Send(pid, msg)
}

func (h *gateHandler) OnClose(c kknet.IConn, err error) {
	// 获取连接对应的 PID 并停止
	pid, ok := h.gate.router.GetPID(c.ID())
	if ok {
		h.gate.actorSys.Root.Stop(pid)
	}

	kklog.Infof("[ccgate] client disconnected: connID=%d, remoteAddr=%s, err=%v", c.ID(), c.RemoteAddr(), err)
}
