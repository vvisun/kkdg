package ccgate

import (
	"errors"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/component"
	"github.com/vvisun/kkdg/kkapp/transport"
	"github.com/vvisun/kkdg/kkapp/transport/gatetrans"
	"github.com/vvisun/kkdg/kkapp/transport/gatetrans/transnat"
	"github.com/vvisun/kkdg/kkapp/transport/gatetrans/transrpc"
	"github.com/vvisun/kkdg/kkapp/transport/gatetrans/transshard"
	"github.com/vvisun/kkdg/kkapp/transport/ptotrans"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkgws"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kkprocessor"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/remotes/kkcluster"
	"github.com/vvisun/kkdg/remotes/kkcluster/cnats"
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
	"github.com/vvisun/kkdg/remotes/kkdiscovery/dnats"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/kkoption"
)

// 网关服
type gateComponent struct {
	component.Component
	opt       Option
	serverOpt kknet.Options
	server    kknet.IServer
	handler   *gateHandler

	discovery   kkdiscovery.IDiscovery
	cluster     kkcluster.ICluster // cluster for forwarding messages to logic and client
	transportor gatetrans.ITransportor
	clientMgr   *clientManager
	sessionMgr  gatetrans.ISessionManager
}

func (slf *gateComponent) GetCompName() string {
	return "comp_gate"
}

var _ kkapp.IComponent = (*gateComponent)(nil)

var _ actor.Actor = (*gateComponent)(nil)

func (slf *gateComponent) Receive(context actor.Context) {
	switch context.Message().(type) {
	case *actor.Stopping:
		slf.OnStop()
	}
}

// NewGateComponent creates a new gate component.
func NewGateComponent(gateOpt Option, serverOpt kknet.Options) *gateComponent {
	if err := validateOption(&gateOpt); err != nil {
		kklog.PanicErr(err)
	}
	return &gateComponent{
		opt:        gateOpt,
		serverOpt:  serverOpt,
		sessionMgr: gatetrans.NewSessionMgr(),
		clientMgr:  newClientManager(),
	}
}

func (slf *gateComponent) OnInit() error {
	// 创建 handler
	slf.handler = newGateHandler(slf)

	// 初始化 discovery
	discoveryOpts := dnats.ApplyNatsOptions(dnats.WithUrl(slf.opt.DiscoveryUrl))
	slf.discovery = dnats.NewNatsDiscovery(
		"gate."+slf.GetApplication().GetNodeId(),
		slf.GetApplication().GetNodeInfo(),
		discoveryOpts,
		kkdiscovery.ApplyOptions(),
	)

	// 初始化 cluster（用于 gate <-> logic 转发）
	clusterOpts := cnats.ApplyNatsOptions(cnats.WithUrl(slf.opt.ClusterUrl))
	slf.cluster = cnats.NewNatsCluster(
		slf.GetApplication().GetNodeId(),
		slf.GetApplication().GetNodeType(),
		slf.discovery,
		clusterOpts,
		kkcluster.ApplyOptions(),
	)

	appOpts := slf.GetApplication().GetOptions()
	nodeInfo := slf.GetApplication().GetNodeInfo()

	// 初始化 transportor
	switch slf.opt.TransType {
	case transport.TransTypeNats:
		transportor, err := transnat.NewTransportorNats(
			slf.cluster, slf.sessionMgr, appOpts.TransMsgPacket, appOpts.StreamTool)
		if err != nil {
			return err
		}
		slf.transportor = transportor
	case transport.TransTypeRpc:
		transportor, err := transrpc.NewTransportorRpc(
			slf.sessionMgr, nodeInfo.GetNodeId(), slf.opt.RpcAddr)
		if err != nil {
			return err
		}
		slf.transportor = transportor
	case transport.TransTypeShard:
		transportor, err := transshard.NewTransportorShard(
			slf.opt.RpcAddr, slf.sessionMgr, nodeInfo.GetNodeId(),
			appOpts.TransMsgPacket, appOpts.ClientMsgPacket, appOpts.StreamTool, appOpts.StreamTool)
		if err != nil {
			return err
		}
		slf.transportor = transportor
	default:
		return errors.New("invalid trans type: " + slf.opt.TransType)
	}

	slf.transportor.HookMsg(func(msgId kkpacket.MSGID, data any) {
		switch msgId {
		case ptotrans.MsgIDRpcClientLoginLogout:
			msg, ok := data.(*ptotrans.RpcClientLoginLogout)
			if !ok {
				return
			}
			kklog.Infof("[ccgate] notify client login logout: %v", msg)
			if msg.IsLogin {
				slf.clientMgr.loginToLogicNode(msg.ClientId, msg.NodeType, kknet.USER_ID(msg.UserId))
			} else {
				slf.clientMgr.logoutFromLogicNode(msg.ClientId, msg.NodeType)
				gLogicTotalMgr.onUnbindLogicNode(msg.ClientId, msg.NodeId)
			}
		}
	})

	return nil
}

func (slf *gateComponent) OnStart() error {
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

func (slf *gateComponent) OnStop() error {
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

	// 停止 transportor
	if slf.transportor != nil {
		if err := slf.transportor.Stop(); err != nil {
			kklog.Errorf("[ccgate] stop transportor error: %v", err)
		}
	}

	return nil
}

func (slf *gateComponent) startTCPServer() error {
	// 创建 TCP 服务器
	opts := slf.serverOpt
	kkoption.ApplyOptionsTo(&opts,
		kknet.WithStreamTool(slf.GetApplication().GetOptions().StreamTool),
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
	opts := slf.serverOpt
	appOpts := slf.GetApplication().GetOptions()
	kkoption.ApplyOptionsTo(&opts,
		kknet.WithStreamTool(appOpts.StreamTool),
		kknet.WithMsgPacket(appOpts.ClientMsgPacket),
		kknet.WithRawHandler(slf.handler),
		kknet.WithWorkerQueueMaxConcurrency(1),
		kknet.WithBufferSizes(2*1024, 2*1024),
		kknet.WithRecvQueueSize(128),
		kknet.WithRecvQueueStrict(true),
		kknet.WithRecvBufShrinkCap(2*1024),
		kknet.WithRecvQueueFullCallback(slf.opt.RecvQueueFullCallback),
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

// 选择逻辑节点的唯一入口。
func (slf *gateComponent) chooseLogicNode(nodeType string) (string, bool) {
	if slf.opt.TransType == transport.TransTypeShard || slf.opt.TransType == transport.TransTypeRpc {
		return slf.chooseFromShardOrRpc(nodeType)
	}
	return slf.chooseFromDiscovery(nodeType)
}

// 从shard中选择权重最小的逻辑节点. return nodeId, found
func (slf *gateComponent) chooseFromShardOrRpc(nodeType string) (string, bool) {
	trans := slf.transportor.(gatetrans.IMemberMgrGetter)
	if trans == nil {
		return "", false
	}
	var chooseNode gatetrans.IMember = nil
	finded := false
	memberMgr := trans.GetMemberMgr()
	memberMgr.Range(func(nodeId string, member gatetrans.IMember) bool {
		if member.GetNodeType() != nodeType {
			return true
		}
		if chooseNode == nil {
			chooseNode = member
			finded = true
			return true
		}
		if gLogicTotalMgr.getSessionCount(member.GetNodeID()) < gLogicTotalMgr.getSessionCount(chooseNode.GetNodeID()) {
			chooseNode = member
			finded = true
		}
		return true
	})
	if finded {
		return chooseNode.GetNodeID(), true
	}
	return "", false
}

// 从discovery中选择权重最小的逻辑节点. return nodeId, found
func (slf *gateComponent) chooseFromDiscovery(nodeType string) (string, bool) {
	if slf.discovery == nil {
		return "", false
	}
	if slf.discovery.GetMemberMgr().CountOfType(nodeType) == 0 {
		return "", false
	}

	var chooseNode kkdiscovery.IMember = nil
	finded := false
	slf.discovery.GetMemberMgr().RangeType(nodeType, func(nodeID string, member kkdiscovery.IMember) bool {
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
	if finded {
		return chooseNode.GetNodeID(), true
	}
	return "", false
}

//------------------------------------------------------------

type gateHandler struct {
	gate   *gateComponent
	wQueue *kkprocessor.WorkerQueue
}

var _ kknet.IConnLifecycleHandler = (*gateHandler)(nil)
var _ kknet.IRawHandler = (*gateHandler)(nil)

func newGateHandler(gate *gateComponent) *gateHandler {
	return &gateHandler{
		gate:   gate,
		wQueue: kkprocessor.NewWorkerQueue(2),
	}
}

func (h *gateHandler) OnConnect(c kknet.IConn) {
	sessionID := getSessionId(c.ID(), h.gate.GetApplication().GetNodeId())
	h.gate.sessionMgr.AddConn(sessionID, c)
	h.gate.clientMgr.addClient(c.ID(), sessionID)
	kklog.Debugf("[ccgate] client connected: connID=%d, remoteAddr=%s", c.ID(), c.RemoteAddr())
}

func (h *gateHandler) OnClose(c kknet.IConn, err error) {
	cid := c.ID()
	sid := getSessionId(cid, h.gate.GetApplication().GetNodeId())

	// 在 removeClient 前取出该客户端已分配的逻辑服 nodeId，用于通知断开
	if cliInfo := h.gate.clientMgr.getClient(cid); cliInfo != nil {
		cliInfo.rangeLogicNodes(func(nodeType string, lgcInfo *clientLogicItem) bool {
			if lgcInfo.nodeId != "" {
				logicNodeId := lgcInfo.nodeId
				h.wQueue.Push(func() {
					if h.gate != nil && h.gate.transportor != nil {
						h.gate.transportor.NotifyClientDisconnect(sid, logicNodeId, cid)
					}
				})
			}
			return true
		})
	}

	h.gate.sessionMgr.RemoveConn(sid)
	h.gate.clientMgr.removeClient(cid)
	kklog.Debugf("[ccgate] client disconnected: connID=%d, remoteAddr=%s, err=%v", cid, c.RemoteAddr(), err)
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

	appOpts := h.gate.GetApplication().GetOptions()
	// Best-effort: derive route from msgID if it is registered.
	msgBytes, err := appOpts.StreamTool.MessageBytes(data.B)
	if err != nil {
		kklog.Warnf("[ccgate] get message bytes error: %v", err)
		return
	}
	msgID, err := appOpts.ClientMsgPacket.GetMsgID(msgBytes)
	if err != nil {
		kklog.Warnf("[ccgate] get message id error: %v", err)
		return
	}
	route, err := appOpts.ClientMsgPacket.GetRouter().GetMsgRoute(msgID)
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

	// 将客户端消息原样转发给逻辑服
	streamBytes := data.B //transportor编码时是复制，所以这里可以直接传引用，不用再复制一次。
	sessionID := cliInfo.sessionId
	if err := h.gate.transportor.ForwardToLogic(sessionID, streamBytes, logicNode.nodeId); err != nil {
		kklog.Warnf("[ccgate] forward to logic error: %v", err)
	}
	kkbuffer.Put(data)
}
