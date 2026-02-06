package ccgate

import (
	"strconv"
	"sync"

	"github.com/nats-io/nats.go"
	"github.com/vvisun/kkdg/kkapp/component"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkcluster"
	"github.com/vvisun/kkdg/kknet/kkcluster/cnats"
	"github.com/vvisun/kkdg/kknet/kkdiscovery"
	"github.com/vvisun/kkdg/kknet/kkdiscovery/dnats"
	"github.com/vvisun/kkdg/kknet/kkpacket"
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
	cluster   kkcluster.ICluster

	// sessionID(string) -> kknet.IConn
	connMap   sync.Map
	msgRouter *kkpacket.Router
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
	// defaults
	if slf.opt.LogicNodeType == "" {
		slf.opt.LogicNodeType = "logic"
	}
	if slf.opt.NatsURL == "" {
		if v, ok := slf.GetApplication().GetNodeInfo().GetSetting("nats_url"); ok {
			slf.opt.NatsURL = v
		}
	}

	// 创建 handler
	slf.handler = newGateHandler(slf)

	// 初始化 discovery
	nodeInfo1 := slf.GetApplication().GetNodeInfo()
	var discoveryOpts []nats.Option
	if slf.opt.NatsURL != "" {
		discoveryOpts = append(discoveryOpts, dnats.WithUrl(slf.opt.NatsURL))
	}
	slf.discovery = dnats.NewNatsDiscovery("gate."+slf.GetApplication().GetNodeId(), nodeInfo1, nil, discoveryOpts...)

	// 初始化 cluster（用于 gate <-> logic 转发）
	var clusterOpts []nats.Option
	if slf.opt.NatsURL != "" {
		clusterOpts = append(clusterOpts, cnats.WithUrl(slf.opt.NatsURL))
	}
	slf.cluster = cnats.NewNatsCluster(
		slf.GetApplication().GetNodeId(),
		slf.GetApplication().GetNodeType(),
		slf.discovery,
		clusterOpts...,
	)
	slf.cluster.SetPublishHandler(slf.onClusterPublish)

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

	// 启动 cluster
	if slf.cluster != nil {
		if err := slf.cluster.Init(); err != nil {
			return err
		}
	}

	return nil
}

func (slf *gateComponent) Stop() error {
	// 停止 cluster
	if slf.cluster != nil {
		slf.cluster.Stop()
	}

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

// ForwardToLogic implements ITransportor. It forwards the raw message bytes ([message]) to logic nodes.
func (slf *gateComponent) ForwardToLogic(sessionID string, msgRoute string, msgBytes []byte) error {
	if slf.cluster == nil {
		return ErrClusterNotInitialized
	}
	if sessionID == "" {
		return ErrEmptySessionID
	}
	if len(msgBytes) == 0 {
		return nil
	}

	pkt := kkcluster.NewClusterPacket()
	pkt.FuncName = msgRoute
	pkt.ArgBytes = append([]byte(nil), msgBytes...)
	pkt.Session = &kkcluster.Session{
		Sid: sessionID,
	}
	return slf.cluster.PublishRemoteType(slf.opt.LogicNodeType, pkt)
}

func (slf *gateComponent) onClusterPublish(_ string, packet *kkcluster.ClusterPacket) {
	if packet == nil || packet.Session == nil || packet.Session.Sid == "" {
		return
	}
	v, ok := slf.connMap.Load(packet.Session.Sid)
	if !ok {
		return
	}
	conn, ok := v.(kknet.IConn)
	if !ok || conn == nil {
		return
	}
	if len(packet.ArgBytes) == 0 {
		return
	}

	// packet.ArgBytes is [message], pack it to [length,message] then send back to client.
	bb, err := kkpacket.DefaultStreamPacket().Pack(packet.ArgBytes)
	if err != nil {
		kklog.Errorf("[ccgate] pack response error: %v", err)
		return
	}
	if err := conn.SendBuffer(bb); err != nil {
		kklog.Errorf("[ccgate] send response error: %v", err)
	}
}

func (slf *gateComponent) startTCPServer() error {
	// 创建 TCP 服务器
	opts := kknet.ApplyOptions(
		kknet.WithLogger(kklog.Stdout()),
		kknet.WithRawHandler(slf.handler),
	)
	server := kktcp.NewServer(slf.opt.TCPAddr, slf.handler, opts)

	if err := server.Start(); err != nil {
		return err
	}

	slf.server = server

	kklog.Infof("[ccgate] tcp server started on %s", slf.opt.TCPAddr)
	return nil
}

func (slf *gateComponent) startWSServer() error {
	// 创建 WebSocket 服务器
	opts := kknet.ApplyOptions(
		kknet.WithLogger(kklog.Stdout()),
		kknet.WithRawHandler(slf.handler),
	)
	server := kkws.NewServer(slf.opt.WSAddr, slf.handler, opts)

	if err := server.Start(); err != nil {
		return err
	}

	slf.server = server

	kklog.Infof("[ccgate] ws server started on %s", slf.opt.WSAddr)
	return nil
}

type gateHandler struct {
	gate *gateComponent
}

var _ kknet.IConnLifecycleHandler = (*gateHandler)(nil)
var _ kknet.IRawHandler = (*gateHandler)(nil)

func newGateHandler(gate *gateComponent) *gateHandler {
	return &gateHandler{
		gate: gate,
	}
}

func (h *gateHandler) OnConnect(c kknet.IConn) {
	h.gate.connMap.Store(strconv.FormatInt(c.ID(), 10), c)
	kklog.Infof("[ccgate] client connected: connID=%d, remoteAddr=%s", c.ID(), c.RemoteAddr())
}

func (h *gateHandler) OnClose(c kknet.IConn, err error) {
	h.gate.connMap.Delete(strconv.FormatInt(c.ID(), 10))
	kklog.Infof("[ccgate] client disconnected: connID=%d, remoteAddr=%s, err=%v", c.ID(), c.RemoteAddr(), err)
}

func (h *gateHandler) OnRaw(connID kknet.CONN_ID, data buffers.IBuffer) {
	if data == nil || len(data.Bytes()) == 0 {
		return
	}

	// server handler gives us a frame [length,message]. unpack to [message].
	msgBytes, err := kkpacket.DefaultStreamPacket().Unpack(data.Bytes())
	if err != nil {
		kklog.Errorf("[ccgate] unpack stream packet error: %v", err)
		return
	}

	// Best-effort: derive route from msgID if it is registered.
	msgID, _, err := kkpacket.ParseMsgInfo(msgBytes, kkpacket.DefaultStreamPacket().GetMessagePacket())
	route := ""
	if err == nil {
		route = h.gate.msgRouter.GetMsgRoute(msgID)
	}

	sessionID := strconv.FormatInt(connID, 10)
	if err := h.gate.ForwardToLogic(sessionID, route, msgBytes); err != nil {
		kklog.Errorf("[ccgate] forward to logic error: %v", err)
	}
}
