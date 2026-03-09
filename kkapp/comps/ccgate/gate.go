package ccgate

import (
	"errors"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/component"
	"github.com/vvisun/kkdg/kkapp/comps/ccgate/gatetrans"
	"github.com/vvisun/kkdg/kkapp/comps/ccgate/gatetrans/transnat"
	"github.com/vvisun/kkdg/kkapp/comps/ccgate/gatetrans/transrpc"
	"github.com/vvisun/kkdg/kkapp/comps/ccgate/gatetrans/transshard"
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

	clientMgr   *clientManager
	transportor gatetrans.ITransportor
	cluster     kkcluster.ICluster // cluster for forwarding messages to logic and client
	sessionMgr  gatetrans.ISessionManager
}

func (slf *gateComponent) GetID() string {
	return "gate"
}

var _ component.IComponent = (*gateComponent)(nil)

// NewGateComponent creates a new gate component.
func NewGateComponent(opt Option) *gateComponent {
	if err := validateOption(&opt); err != nil {
		panic(err)
	}
	return &gateComponent{
		opt:        opt,
		sessionMgr: gatetrans.NewSessionMgr(),
		clientMgr:  newClientManager(),
	}
}

func (slf *gateComponent) Init() error {
	// defaults
	if slf.opt.LogicNodeType == "" {
		slf.opt.LogicNodeType = kkapp.NodeTypeLogic
	}
	if slf.opt.TCPAddr == "" && slf.opt.WSAddr == "" {
		return errors.New("tcp addr or ws addr is required")
	}

	// 创建 handler
	slf.handler = newGateHandler(slf)

	// 初始化 discovery
	discoveryOpts := dnats.ApplyNatsOptions(dnats.WithUrl(slf.opt.DiscoveryUrl))
	slf.discovery = dnats.NewNatsDiscovery(
		"gate."+slf.GetApplication().GetNodeId(),
		slf.GetApplication().GetNodeInfo(),
		nil,
		discoveryOpts,
	)

	// 初始化 cluster（用于 gate <-> logic 转发）
	clusterOpts := cnats.ApplyNatsOptions(cnats.WithUrl(slf.opt.ClusterUrl))
	slf.cluster = cnats.NewNatsCluster(
		slf.GetApplication().GetNodeId(),
		slf.GetApplication().GetNodeType(),
		slf.discovery,
		clusterOpts,
	)

	// 初始化 transportor
	switch slf.opt.TransType {
	case kkapp.TransTypeNats:
		slf.transportor = transnat.NewTransportorNats(slf.cluster, slf.sessionMgr)
	case kkapp.TransTypeRpc:
		slf.transportor = transrpc.NewTransportorRpc(slf.sessionMgr, slf.GetApplication().GetNodeId(), slf.opt.RpcAddr)
	case kkapp.TransTypeShard:
		slf.transportor = transshard.NewTransportorShard(slf.opt.RpcAddr, slf.sessionMgr, slf.GetApplication().GetNodeId())
	default:
		return errors.New("invalid trans type: " + slf.opt.TransType)
	}

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
		kknet.WithWorkerQueueMaxConcurrency(1),
		kknet.WithBufferSizes(2*1024, 2*1024),
		kknet.WithRecvQueueSize(128),
		kknet.WithRecvQueueStrict(true),
		kknet.WithRecvBufShrinkCap(2*1024),
		kknet.WithRecvQueueFullCallback(slf.opt.RecvQueueFullCallback),
		kknet.WithMsgPacket(kkapp.GetMsgPacket()),
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
func (slf *gateComponent) allocLogicNode(connID kknet.CONN_ID, nodeType string) *clientLogicItem {
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

	// 选择逻辑节点
	chooseNode, found := slf.chooseLogicNode(nodeType)
	if !found {
		return nil //没有找到合适的逻辑节点
	}

	// 分配逻辑节点
	return cliInfo.bindLogicNode(nodeType, chooseNode)
}

func (slf *gateComponent) chooseLogicNode(nodeType string) (string, bool) {
	if slf.opt.TransType == kkapp.TransTypeShard {
		if slf.discovery != nil && slf.discovery.IsRunning() {
			return slf.chooseFromDiscovery(nodeType)
		}
		return slf.chooseFromShard(nodeType)
	}
	return slf.chooseFromDiscovery(nodeType)
}

// 从shard中选择权重最小的逻辑节点. return nodeId, found
func (slf *gateComponent) chooseFromShard(nodeType string) (string, bool) {
	if slf.opt.TransType == kkapp.TransTypeShard {
		trans := slf.transportor.(*transshard.TransportorShard)
		if trans != nil {
			return trans.ChooseLogicServer(nodeType, gLogicTotalMgr)
		}
	}
	return "", false
}

// 从discovery中选择权重最小的逻辑节点. return nodeId, found
func (slf *gateComponent) chooseFromDiscovery(nodeType string) (string, bool) {
	var chooseNode kkdiscovery.IMember = nil
	finded := false
	slf.discovery.GetMemberMgr().Range(func(nodeID string, member kkdiscovery.IMember) bool {
		if member.GetNodeType() != nodeType {
			return true
		}
		if chooseNode == nil {
			chooseNode = member
			finded = true
			return true
		}
		if member.GetWeight() < chooseNode.GetWeight() {
			chooseNode = member
			finded = true
		}
		return true
	})
	if chooseNode == nil || !finded {
		return "", false
	}
	return chooseNode.GetNodeID(), true
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
	h.gate.sessionMgr.AddConn(sessionID, c)
	h.gate.clientMgr.addClient(c.ID(), sessionID)
	kklog.Debugf("[ccgate] client connected: connID=%d, remoteAddr=%s", c.ID(), c.RemoteAddr())
}

func (h *gateHandler) OnClose(c kknet.IConn, err error) {
	sid := getSessionId(c.ID(), h.gate.GetApplication().GetNodeId())
	cid := c.ID()
	// 在 removeClient 前取出该客户端已分配的逻辑服 nodeId，用于通知断开
	var logicNodeId string
	if cliInfo := h.gate.clientMgr.getClient(c.ID()); cliInfo != nil {
		if lgc := cliInfo.getLogicNode(h.gate.opt.LogicNodeType); lgc != nil {
			logicNodeId = lgc.nodeId
		}
	}
	go func() {
		if logicNodeId != "" {
			h.gate.transportor.NotifyClientDisconnect(sid, logicNodeId, cid)
		}
	}()
	h.gate.sessionMgr.RemoveConn(sid)
	h.gate.clientMgr.removeClient(c.ID())
	kklog.Debugf("[ccgate] client disconnected: connID=%d, remoteAddr=%s, err=%v", c.ID(), c.RemoteAddr(), err)
}

// OnRaw 收到客户端消息，转发给逻辑节点
func (h *gateHandler) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	if data == nil || len(data.Bytes()) == 0 {
		return
	}

	cliInfo := h.gate.clientMgr.getClient(connID)
	if cliInfo == nil {
		kkbuffer.Put(data)
		return
	}

	// Best-effort: derive route from msgID if it is registered.
	msgBytes, err := kkpacket.DefaultStreamPacket().MessageBytes(data.B)
	if err != nil {
		kklog.Warnf("[ccgate] get message bytes error: %v", err)
		return
	}
	msgID, err := kkapp.GetMsgPacket().GetMsgID(msgBytes)
	if err != nil {
		kklog.Warnf("[ccgate] get message id error: %v", err)
		return
	}
	route, err := kkapp.GetMsgPacket().GetRouter().GetMsgRoute(msgID)
	if err != nil {
		kklog.Warnf("[ccgate] get message route error: %v", err)
		return
	}

	// 这里应该先为client选择一个逻辑服
	logicNode := h.gate.allocLogicNode(connID, route)
	if logicNode == nil {
		// kklog.Debugf("[ccgate] alloc logic node failed")
		// tell busy
		if h.gate.opt.RecvQueueFullCallback != nil {
			conn := h.gate.server.GetConnManager().GetConn(connID)
			if conn != nil {
				h.gate.opt.RecvQueueFullCallback(conn)
			}
		}
		return
	}

	streamBytes := data.B //transportor编码时是复制，所以这里可以直接传引用，不用再复制一次。
	sessionID := cliInfo.sessionId
	if err := h.gate.transportor.ForwardToLogic(sessionID, streamBytes, logicNode.nodeId); err != nil {
		kklog.Warnf("[ccgate] forward to logic error: %v", err)
	}
	kkbuffer.Put(data)
}
