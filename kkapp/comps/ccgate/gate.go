package ccgate

import (
	"errors"
	"strconv"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/component"
	"github.com/vvisun/kkdg/kkapp/transport"
	"github.com/vvisun/kkdg/kkapp/transport/gatetrans"
	"github.com/vvisun/kkdg/kkapp/transport/gatetrans/transnat"
	"github.com/vvisun/kkdg/kkapp/transport/gatetrans/transrpc"
	"github.com/vvisun/kkdg/kkapp/transport/gatetrans/transshard"
	"github.com/vvisun/kkdg/kkapp/transport/ptotrans"
	"github.com/vvisun/kkdg/kkapp/user"
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

//go:inline
func getSessionId(connID kknet.CONN_ID, gateNodeId string) string {
	return gateNodeId + "-" + strconv.FormatUint(connID, 10)
}

// 网关服
//
//	连接管理: kknet.IConnManager connId -> kknet.IConn
//	会话管理: gatetrans.ISessionManager sessionId -> kknet.IConn
//	用户管理: userManager user.USER_ID -> *clientInfo
//	客户端管理: clientManager connId,sessionId -> *clientInfo
//
// 说明：
//   - 由于网关与逻辑服之间是多对多的，即多个逻辑服可以连接到同一个网关，客户端也可能选择不同的网关登入到逻辑服
//     所以同一个逻辑服也可能连接到多个网关，因此需要使用sessionId来区分不同的客户端。
//   - 因为网关上的connId是本服全局唯一，节点ID是节点的唯一标识
//     所以sessionId的生成方式为【gateNodeId + "-" + connId】，这样即可保证sessionId的唯一性。
//   - 原本逻辑服可以直接根据sessionId生成规则来得知消息源自哪个gate，但是未来可能sessionId的生成规则会改变，
//     所以为了通用性，并没有采取这样的做法，而是通过转发协议来得知，详见[ptotrans.RpcC2S]。
type gateComponent struct {
	component.Component
	opt       Option
	serverOpt kknet.Options
	server    kknet.IServer
	handler   *gateHandler

	discovery   kkdiscovery.IDiscovery
	cluster     kkcluster.ICluster // cluster for forwarding messages to logic and client
	transportor gatetrans.ITransportor

	sessionMgr    gatetrans.ISessionManager
	clientMgr     *clientManager
	userMgr       *userManager
	logicTotalMgr *logicTotalManager
}

// NewGateComponent creates a new gate component.
func NewGateComponent(gateOpt Option, serverOpt kknet.Options) *gateComponent {
	if err := validateOption(&gateOpt); err != nil {
		kklog.PanicErr(err)
	}
	return &gateComponent{
		opt:           gateOpt,
		serverOpt:     serverOpt,
		sessionMgr:    gatetrans.NewSessionMgr(),
		clientMgr:     newClientManager(),
		userMgr:       newUserManager(),
		logicTotalMgr: newLogicTotalManager(),
	}
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
			if msg.IsLogin {
				kklog.Infof("[ccgate]客户端登录: %#v", msg)
				cliInfo := slf.clientMgr.getClientBySessionId(msg.ClientId)
				if cliInfo != nil {
					bindTbl := cliInfo.clientBindTbl
					if bindTbl != nil {
						if lgcInfo := bindTbl.getLogicItem(msg.NodeType); lgcInfo != nil {
							lgcInfo.userId = user.USER_ID(msg.UserId)
						}
					}
					kickList := slf.userMgr.addUser(user.USER_ID(msg.UserId), msg.ClientId, bindTbl)
					if slf.opt.UserKickedCallback != nil && len(kickList) > 0 {
						slf.opt.UserKickedCallback(user.USER_ID(msg.UserId), kickList)
					}
				}
			} else {
				kklog.Infof("[ccgate]客户端登出: %#v", msg)
				bindTbl := slf.userMgr.getUserBindTable(user.USER_ID(msg.UserId))
				if bindTbl != nil {
					if lgcInfo := bindTbl.getLogicItem(msg.NodeType); lgcInfo != nil {
						lgcInfo.logout()
					}
					bindTbl.unbindLogicItem(msg.NodeType)
				}
				slf.logicTotalMgr.onUnbindLogicNode(msg.ClientId, msg.NodeId)
				slf.userMgr.removeUser(user.USER_ID(msg.UserId))
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
	cliInfo := slf.clientMgr.getClientByConnId(connID)
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
	slf.logicTotalMgr.onBindLogicNode(cliInfo.sessionId, chooseNode, nodeType)
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
	logicTotalMgr := slf.logicTotalMgr
	memberMgr.Range(func(nodeId string, member gatetrans.IMember) bool {
		if member.GetNodeType() != nodeType {
			return true
		}
		if chooseNode == nil {
			chooseNode = member
			finded = true
			return true
		}
		if logicTotalMgr.getSessionCount(member.GetNodeID()) < logicTotalMgr.getSessionCount(chooseNode.GetNodeID()) {
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
	gate       *gateComponent
	wQueue     *kkprocessor.WorkerQueue
	gateNodeId string
}

var _ kknet.IConnLifecycleHandler = (*gateHandler)(nil)
var _ kknet.IRawHandler = (*gateHandler)(nil)

func newGateHandler(gate *gateComponent) *gateHandler {
	return &gateHandler{
		gate:       gate,
		wQueue:     kkprocessor.NewWorkerQueue(2),
		gateNodeId: gate.GetApplication().GetNodeId(),
	}
}

func (h *gateHandler) OnConnect(c kknet.IConn) {
	sessionID := getSessionId(c.ID(), h.gateNodeId)
	h.gate.sessionMgr.AddConn(sessionID, c)
	h.gate.clientMgr.addClient(c.ID(), sessionID)
	kklog.Debugf("[ccgate] client connected: connID=%d, remoteAddr=%s", c.ID(), c.RemoteAddr())
}

func (h *gateHandler) OnClose(c kknet.IConn, err error) {
	cid := c.ID()
	sid := getSessionId(cid, h.gateNodeId)

	// 通知所有已绑定的逻辑服，网关处该客户端连接已断开
	if cliInfo := h.gate.clientMgr.getClientByConnId(cid); cliInfo != nil {
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
	h.gate.userMgr.onSessionDisconnect(sid)
	kklog.Debugf("[ccgate] client disconnected: connID=%d, remoteAddr=%s, err=%v", cid, c.RemoteAddr(), err)
}

// OnRaw 收到客户端消息，转发给逻辑节点
func (h *gateHandler) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	if data == nil || len(data.Bytes()) == 0 {
		return
	}
	defer kkbuffer.Put(data)

	cliInfo := h.gate.clientMgr.getClientByConnId(connID)
	if cliInfo == nil {
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
		// 通知客户端分配逻辑服失败
		if h.gate.opt.AllocLogicNodeFailedCallback != nil {
			conn := h.gate.server.GetConnManager().GetConn(connID)
			if conn != nil {
				h.gate.opt.AllocLogicNodeFailedCallback(conn)
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
}
