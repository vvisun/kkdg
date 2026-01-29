package ccgate

import (
	"github.com/vvisun/kkdg/kkapp/component"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkdiscovery"
	"github.com/vvisun/kkdg/kknet/kkdiscovery/dnats"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/kknet/kkws"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/kklog"
)

// 网关服
type gateComponent struct {
	component.Component
	opt       Option
	server    kknet.IServer
	handler   *gateHandler
	discovery kkdiscovery.IDiscovery
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
	// 创建 handler
	slf.handler = newGateHandler(slf)

	// 初始化 discovery
	nodeInfo1 := slf.GetApplication().GetNodeInfo()
	slf.discovery = dnats.NewNatsDiscovery("gate."+slf.GetApplication().GetNodeId(), nodeInfo1, nil)

	return nil
}

func (slf *gateComponent) Start() error {
	if slf.opt.TCPAddr != "" {
		if err := slf.startTCPServer(); err != nil {
			return err
		}
	} else if slf.opt.WSAddr != "" {
		if err := slf.startWSServer(); err != nil {
			return err
		}
	}

	// 启动 discovery
	if err := slf.discovery.Start(); err != nil {
		return err
	}

	return nil
}

func (slf *gateComponent) Stop() error {
	// 停止 discovery
	if slf.discovery != nil {
		if err := slf.discovery.Stop(); err != nil {
			kklog.Errorf("[ccgate] stop discovery error: %v", err)
		}
	}

	// 停止服务器
	if slf.server != nil {
		if err := slf.server.Stop(); err != nil {
			kklog.Errorf("[ccgate] stop tcp server error: %v", err)
		}
	}

	return nil
}

func (slf *gateComponent) startTCPServer() error {
	// 创建 TCP 服务器
	server := kktcp.NewServer(
		slf.opt.TCPAddr,
		slf.handler,
		kknet.WithLogger(kklog.Stdout()),
	)

	if err := server.Start(); err != nil {
		return err
	}

	slf.server = server

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

	slf.server = server

	kklog.Infof("[ccgate] ws server started on %s", slf.opt.WSAddr)
	return nil
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
	kklog.Infof("[ccgate] client connected: connID=%d, remoteAddr=%s", c.ID(), c.RemoteAddr())
}

func (h *gateHandler) OnMessage(c kknet.IConn, data buffers.IBuffer) {

}

func (h *gateHandler) OnClose(c kknet.IConn, err error) {
	kklog.Infof("[ccgate] client disconnected: connID=%d, remoteAddr=%s, err=%v", c.ID(), c.RemoteAddr(), err)
}
