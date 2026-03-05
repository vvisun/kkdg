package ccgate

import (
	"errors"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/component"
	"github.com/vvisun/kkdg/kkapp/comps/ccgate/transface"
	"github.com/vvisun/kkdg/kkapp/comps/ccgate/transnat"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkgws"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/remotes/kkcluster"
	"github.com/vvisun/kkdg/remotes/kkcluster/cnats"
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
	"github.com/vvisun/kkdg/remotes/kkdiscovery/dnats"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

// 网关服
type gateComponent struct {
	component.Component
	opt       Option
	server    kknet.IServer
	handler   *gateHandler
	discovery kkdiscovery.IDiscovery

	clientMgr   clientManager
	transportor transface.ITransportor
	cluster     kkcluster.ICluster // cluster for forwarding messages to logic and client
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
		slf.opt.LogicNodeType = kkapp.NodeTypeLogic
	}
	if slf.opt.NatsURL == "" {
		if v, ok := slf.GetApplication().GetNodeInfo().GetSetting("nats_url"); ok {
			slf.opt.NatsURL = v
		}
	}
	if slf.opt.TCPAddr == "" && slf.opt.WSAddr == "" {
		return errors.New("tcp addr or ws addr is required")
	}

	// 创建 handler
	slf.handler = newGateHandler(slf)

	// 初始化 discovery
	nodeInfo1 := slf.GetApplication().GetNodeInfo()
	discoveryOpts := dnats.ApplyNatsOptions(dnats.WithUrl(slf.opt.NatsURL))
	slf.discovery = dnats.NewNatsDiscovery("gate."+slf.GetApplication().GetNodeId(), nodeInfo1, nil, discoveryOpts)

	// 初始化 cluster（用于 gate <-> logic 转发）
	clusterOpts := cnats.ApplyNatsOptions(cnats.WithUrl(slf.opt.NatsURL))
	slf.cluster = cnats.NewNatsCluster(
		slf.GetApplication().GetNodeId(),
		slf.GetApplication().GetNodeType(),
		slf.discovery,
		clusterOpts,
	)
	slf.transportor = transnat.NewTransportorNats(slf.cluster)

	return nil
}

func (slf *gateComponent) Start() error {
	if slf.opt.WSAddr != "" {
		if err := slf.startWSServer(); err != nil {
			return err
		}
	} else if slf.opt.TCPAddr != "" {
		if err := slf.startTCPServer(); err != nil {
			return err
		}
	}

	// 启动 discovery
	if err := slf.discovery.Start(); err != nil {
		return err
	}

	// 启动 cluster
	if slf.cluster != nil {
		if err := slf.cluster.Start(); err != nil {
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

func (slf *gateComponent) startTCPServer() error {
	// 创建 TCP 服务器
	opts := kknet.ApplyOptions(
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
		kknet.WithRawHandler(slf.handler),
	)
	server := kkgws.NewServer(slf.opt.WSAddr, slf.handler, opts)

	if err := server.Start(); err != nil {
		return err
	}

	slf.server = server

	kklog.Infof("[ccgate] ws server started on %s", slf.opt.WSAddr)
	return nil
}

// 为客户端(connID)分配一个nodeType类型的逻辑节点
func (slf *gateComponent) allocLogicNode(connID kknet.CONN_ID, nodeType string) *logicNodeInfo {
	if nodeType == "" {
		return nil //无效的nodeType，不分配逻辑节点
	}
	cliInfo := slf.clientMgr.getClient(connID)
	if cliInfo == nil {
		return nil //客户端不存在，不分配逻辑节点
	}

	// 如果已分配，则返回已分配的逻辑节点信息
	lgcNode := cliInfo.getLogicNode(nodeType)
	if lgcNode != nil {
		return lgcNode
	}

	// 从discovery中选择权重最小的逻辑节点
	var chooseNode kkdiscovery.IMember = nil
	slf.discovery.GetMemberMgr().Range(func(nodeID string, member kkdiscovery.IMember) bool {
		if member.GetNodeType() != nodeType {
			return true
		}
		if chooseNode == nil {
			chooseNode = member
			return true
		}
		if member.GetWeight() < chooseNode.GetWeight() {
			chooseNode = member
		}
		return true
	})
	if chooseNode == nil {
		return nil
	}

	return cliInfo.allocLogicNode(nodeType, chooseNode.GetNodeID())
}

//------------------------------------------------------------

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
	sessionID := getSessionId(c.ID(), h.gate.GetApplication().GetNodeId())
	h.gate.transportor.GetSessionMgr().AddConn(sessionID, c)
	h.gate.clientMgr.addClient(c.ID(), sessionID)
	kklog.Infof("[ccgate] client connected: connID=%d, remoteAddr=%s", c.ID(), c.RemoteAddr())
}

func (h *gateHandler) OnClose(c kknet.IConn, err error) {
	h.gate.transportor.GetSessionMgr().RemoveConn(getSessionId(c.ID(), h.gate.GetApplication().GetNodeId()))
	h.gate.clientMgr.removeClient(c.ID())
	kklog.Infof("[ccgate] client disconnected: connID=%d, remoteAddr=%s, err=%v", c.ID(), c.RemoteAddr(), err)
}

// OnRaw 收到客户端消息，转发给逻辑节点
func (h *gateHandler) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	if data == nil || len(data.Bytes()) == 0 {
		return
	}

	// Best-effort: derive route from msgID if it is registered.
	msgBytes, err := kkpacket.DefaultStreamPacket().MessageBytes(data.B)
	if err != nil {
		kklog.Errorf("[ccgate] get message bytes error: %v", err)
		return
	}
	msgID, err := kkapp.GetMsgPacket().GetMsgID(msgBytes)
	if err != nil {
		kklog.Errorf("[ccgate] get message id error: %v", err)
		return
	}
	route, err := kkapp.GetMsgPacket().GetRouter().GetMsgRoute(msgID)
	if err != nil {
		kklog.Errorf("[ccgate] get message route error: %v", err)
		return
	}

	// 这里应该先为client选择一个逻辑服
	logicNode := h.gate.allocLogicNode(connID, route)
	if logicNode == nil {
		kklog.Errorf("[ccgate] alloc logic node failed")
		return
	}

	streamBytes := data.B //transportor编码时是复制，所以这里可以直接传引用，不用再复制一次。
	sessionID := h.gate.clientMgr.getClient(connID).sessionId
	if err := h.gate.transportor.ForwardToLogic(sessionID, streamBytes, logicNode.nodeId); err != nil {
		kklog.Errorf("[ccgate] forward to logic error: %v", err)
	}
	kkbuffer.Put(data)
}
