package ccgate

import (
	"errors"
	"strconv"
	"time"

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
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/remotes/kkcluster"
	"github.com/vvisun/kkdg/remotes/kkcluster/cnats"
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
	"github.com/vvisun/kkdg/remotes/kkdiscovery/dnats"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/kkoption"
	"github.com/vvisun/kkdg/utils/xcall"
)

//go:inline
func getSessionId(connID kknet.CONN_ID, gateNodeId string) string {
	return gateNodeId + "-" + strconv.FormatUint(connID, 10)
}

// 暴露给业务层使用的会话管理器接口。
// interface for business layer to use.
type ISessionMgr interface {
	// 根据会话ID获取客户端连接。
	// GetConn gets a client connection by sessionID
	GetConn(sessionID string) (kknet.IConn, error)
}

// 网关服
//
//	连接管理: kknet.IConnManager connId -> kknet.IConn
//	会话管理: gatetrans.ISessionManager sessionId -> kknet.IConn
//
//	用户管理: userManager user.USER_ID -> *clientInfo
//	客户端管理: clientManager connId,sessionId -> *clientInfo
//	逻辑节点绑定: logicBindManager sessionId|userId -> nodeType, nodeId
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
	gateOpt   Options
	serverOpt kknet.Options
	server    kknet.IServer
	handler   *gateHandler

	cluster kkcluster.ICluster // cluster for forwarding messages to logic and client

	localDis  *localDiscovery
	discovery kkdiscovery.IDiscovery

	transportor gatetrans.ITransportor
	sessionMgr  gatetrans.ISessionManager

	clientMgr    *clientManager
	userMgr      *userManager
	logicBindMgr *logicBindManager

	errCallback ErrCallback
	feedLimit   *FeedLimit
}

// NewGateComponent creates a new gate component.
func NewGateComponent(gateOpt Options, serverOpt kknet.Options) *gateComponent {
	if err := validateOption(&gateOpt); err != nil {
		kklog.PanicErr(err)
	}
	return &gateComponent{
		gateOpt:      gateOpt,
		serverOpt:    serverOpt,
		localDis:     newLocalDiscovery(),
		sessionMgr:   gatetrans.NewSessionMgr(),
		clientMgr:    newClientManager(),
		userMgr:      newUserManager(),
		logicBindMgr: newLogicBindManager(),
		feedLimit:    NewFeedLimit(time.Second),
	}
}

var _ kkapp.IComponent = (*gateComponent)(nil)

var _ actor.Actor = (*gateComponent)(nil)

// 暴露会话管理器给业务层使用，方便业务层直接操作会话。
func (slf *gateComponent) GetSessionMgr() ISessionMgr {
	return slf.sessionMgr
}

// 暴露连接管理器给业务层使用，方便业务层直接操作连接。
func (slf *gateComponent) GetConnManager() kknet.IConnManager {
	return slf.server.GetConnManager()
}

func (slf *gateComponent) GetCompName() string {
	return "comp_gate"
}

func (slf *gateComponent) Receive(ctx actor.Context) {
	switch ctx.Message().(type) {
	case *actor.Stopping:
		slf.OnStop()
	}
}

func (slf *gateComponent) OnInit() error {
	// 创建 handler
	slf.handler = newGateHandler(slf)

	// 初始化 discovery
	slf.discovery = dnats.NewNatsDiscovery(
		slf.GetApplication().GetNodeInfo(),
		kkdiscovery.ApplyOptions(
			kkdiscovery.WithUrl(slf.gateOpt.DiscoveryUrl),
		),
	)

	// 初始化 cluster（用于 gate <-> logic 转发）
	slf.cluster = cnats.NewNatsCluster(
		slf.GetApplication().GetNodeId(),
		slf.GetApplication().GetNodeType(),
		kkcluster.ApplyOptions(
			kkcluster.WithUrl(slf.gateOpt.ClusterUrl),
			kkcluster.WithDiscovery(slf.discovery),
		),
	)

	appOpts := slf.GetApplication().GetOptions()
	nodeInfo := slf.GetApplication().GetNodeInfo()

	transMsgPacket := kkpacket.NewMessagePacket(
		kkpacket.NewPacketHead(&kkpacket.PartUint32{}),
		appOpts.TransportorCodec,
		kkpacket.NewMsgRouter(),
	)

	// 初始化 transportor
	switch slf.gateOpt.TransType {
	case transport.TransTypeNats:
		transportor, err := transnat.NewTransportorNats(
			slf.cluster,
			slf.sessionMgr,
			transMsgPacket,
			appOpts.StreamTool,
		)
		if err != nil {
			return err
		}
		slf.transportor = transportor
	case transport.TransTypeRpc:
		transportor, err := transrpc.NewTransportorRpc(
			slf.sessionMgr,
			nodeInfo.GetNodeId(),
			slf.gateOpt.TransServerAddr,
		)
		if err != nil {
			return err
		}
		slf.transportor = transportor
	case transport.TransTypeShard:
		transportor, err := transshard.NewTransportorShard(
			slf.gateOpt.TransServerAddr, slf.sessionMgr, nodeInfo.GetNodeId(),
			transMsgPacket,
			appOpts.ClientMsgPacket,
			appOpts.StreamTool,
			appOpts.StreamTool,
		)
		if err != nil {
			return err
		}
		slf.transportor = transportor
	default:
		return errors.New("invalid trans type: " + slf.gateOpt.TransType)
	}

	slf.transportor.HookMsg(func(msgId kkpacket.MSGID, data any) {
		switch msgId {
		case ptotrans.MsgIDRpcClientLoginLogout:
			if msg, ok := data.(*ptotrans.RpcClientLoginLogout); ok {
				slf.loginHook(msg)
			}
		}
	})

	return nil
}

func (slf *gateComponent) OnStart() error {
	if slf.gateOpt.WSAddr != "" {
		if err := slf.startWSServer(); err != nil {
			return err
		}
	} else if slf.gateOpt.TCPAddr != "" {
		if err := slf.startTCPServer(); err != nil {
			return err
		}
	}

	// 启动 discovery
	if slf.discovery != nil {
		if err := slf.discovery.Start(); err != nil {
			return err
		}
	}

	// 启动 cluster
	if slf.cluster != nil {
		if slf.cluster != nil {
			if err := slf.cluster.Start(); err != nil {
				return err
			}
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
	appOpts := slf.GetApplication().GetOptions()
	kkoption.ApplyOptionsTo(&opts,
		kknet.WithStreamTool(appOpts.StreamTool),
		kknet.WithMsgPacket(appOpts.ClientMsgPacket),
		kknet.WithRawHandler(slf.handler),
		kknet.WithRecvQueueFullCallback(func(conn kknet.IConn) {
			slf.feedCallback(conn.ID(), ERR_RECV_QUEUE_FULL)
		}),
	)
	server := kktcp.NewServer(slf.gateOpt.TCPAddr, slf.handler, opts)

	if err := server.Start(); err != nil {
		return err
	}

	slf.server = server

	kklog.Infof("[ccgate] tcp server started on %s", slf.gateOpt.TCPAddr)
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
		kknet.WithRecvQueueStrict(true),
		kknet.WithRecvQueueSize(128),
		kknet.WithRecvBufShrinkCap(2*1024),
		kknet.WithRecvQueueFullCallback(func(conn kknet.IConn) {
			slf.feedCallback(conn.ID(), ERR_RECV_QUEUE_FULL)
		}),
	)
	server := kkgws.NewServer(slf.gateOpt.WSAddr, slf.handler, opts)

	if err := server.Start(); err != nil {
		return err
	}

	slf.server = server

	kklog.Infof("[ccgate] ws server started on %s", slf.gateOpt.WSAddr)
	return nil
}

//------------------------------------------------------------

func (slf *gateComponent) onNewClientConn(c kknet.IConn) {
	sid := getSessionId(c.ID(), slf.GetApplication().GetNodeId())
	slf.sessionMgr.AddConn(sid, c)
	slf.clientMgr.addClient(c.ID(), sid)
}

func (slf *gateComponent) onClientConnClose(c kknet.IConn) {
	cid := c.ID()
	sid := getSessionId(cid, slf.GetApplication().GetNodeId())

	// 通知所有已绑定的逻辑服，网关处该客户端连接已断开
	bindTbl := slf.logicBindMgr.getSessionBindTable(sid)
	if bindTbl != nil {
		bindTbl.rangeLogicItems(func(nodeType string, logicItem *clientLogicItem) bool {
			if logicItem.nodeId != "" {
				logicNodeId := logicItem.nodeId
				xcall.AntsSafeGo(func() {
					if slf.transportor != nil {
						slf.transportor.NotifyClientDisconnect(sid, logicNodeId, cid)
					}
				})
			}
			return true
		})
	}

	slf.sessionMgr.RemoveConn(sid)
	slf.clientMgr.removeClient(cid)
	slf.userMgr.onSessionDisconnect(sid)
	slf.logicBindMgr.onSessionDisconnect(sid)
	slf.feedLimit.Remove(cid)
}

func (slf *gateComponent) loginHook(msg *ptotrans.RpcClientLoginLogout) {
	if msg.IsLogin {
		kklog.Debugf("[ccgate]客户端登录逻辑服成功: %#v", msg)

		slf.logicBindMgr.userBind(user.USER_ID(msg.UserId), msg.NodeType, msg.NodeId)

		kickList := slf.userMgr.onUserLogin(user.USER_ID(msg.UserId), msg.ClientId)
		if slf.errCallback != nil && len(kickList) > 0 {
			for _, kick := range kickList {
				if conn, err := slf.sessionMgr.GetConn(kick.sessionId); err == nil {
					// 从客户端管理器中移除，不再接收被踢连接的消息。
					slf.clientMgr.removeClient(conn.ID())
					// 通知业务层，用户被顶号/被踢出会话。
					slf.feedCallback(conn.ID(), ERR_USER_KICKED)
				}
			}
		}
	} else {
		kklog.Debugf("[ccgate]客户端登出逻辑服成功: %#v", msg)
		slf.logicBindMgr.userUnbind(user.USER_ID(msg.UserId), msg.NodeType)
		slf.localDis.onUnbindLogicNode(msg.ClientId, msg.NodeType, msg.NodeId)
		slf.userMgr.onUserLogout(user.USER_ID(msg.UserId))
	}
}

// 为客户端(connID)分配一个nodeType类型的逻辑节点
func (slf *gateComponent) allocLogicNode(connID kknet.CONN_ID, nodeType string) *clientLogicItem {
	if nodeType == "" {
		return nil //无效的nodeType，不分配逻辑节点
	}

	sessionID := slf.clientMgr.getSessionByConnId(connID)
	if sessionID == "" {
		return nil //客户端不存在|已被踢出会话，不分配逻辑节点
	}

	// 如果已分配，则返回已分配的逻辑节点信息
	if oldLogicItem := slf.logicBindMgr.getLogicItemBySessionId(sessionID, nodeType); oldLogicItem != nil {
		return oldLogicItem
	}

	// 选择逻辑节点
	chooseNodeId, found := slf.chooseLogicNode(nodeType)
	if !found {
		return nil //没有找到合适的逻辑节点
	}

	// 分配逻辑节点
	logicItem := slf.logicBindMgr.sessionBind(sessionID, nodeType, chooseNodeId)
	slf.localDis.onBindLogicNode(sessionID, nodeType, chooseNodeId)
	return logicItem
}

// 选择逻辑节点的唯一入口。
func (slf *gateComponent) chooseLogicNode(nodeType string) (string, bool) {
	if slf.gateOpt.TransType == transport.TransTypeShard || slf.gateOpt.TransType == transport.TransTypeRpc {
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
	lodalDis := slf.localDis
	memberMgr.Range(func(nodeId string, member gatetrans.IMember) bool {
		if member.GetNodeType() != nodeType {
			return true
		}
		if chooseNode == nil {
			chooseNode = member
			finded = true
			return true
		}
		if lodalDis.getMemberWeight(member.GetNodeID()) < lodalDis.getMemberWeight(chooseNode.GetNodeID()) {
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

// 设置错误回调。非线程安全，一般在初始化时设置即可。
func (slf *gateComponent) SetErrCallback(fn ErrCallback) {
	slf.errCallback = fn
}

func (slf *gateComponent) feedCallback(connId kknet.CONN_ID, errCode GateErrorCode) {
	if slf.errCallback == nil || slf.server.GetConnManager().GetConn(connId) == nil {
		return
	}

	if slf.feedLimit.IsLimited(connId, errCode) {
		return // 同一个连接同一个错误码，短时间内只通知一次。
	}
	slf.feedLimit.Reset(connId, errCode)

	xcall.AntsSafeGo(func() {
		if conn := slf.server.GetConnManager().GetConn(connId); conn != nil {
			slf.errCallback(conn, errCode)
		}
	})
}

//------------------------------------------------------------

type gateHandler struct {
	gate       *gateComponent
	gateNodeId string
}

var _ kknet.IConnLifecycleHandler = (*gateHandler)(nil)
var _ kknet.IRawHandler = (*gateHandler)(nil)

func newGateHandler(gate *gateComponent) *gateHandler {
	return &gateHandler{
		gate:       gate,
		gateNodeId: gate.GetApplication().GetNodeId(),
	}
}

func (h *gateHandler) OnConnect(c kknet.IConn) {
	//kklog.Debugf("[ccgate] client connected: connID=%d, remoteAddr=%s", c.ID(), c.RemoteAddr())
	if h.gate.server.GetConnManager().GetCount() >= h.gate.gateOpt.MaxConnCount {
		kklog.Debugf("[ccgate] max conn count reached, reject: remoteAddr=%s", c.RemoteAddr())
		c.Close()
		return
	}
	h.gate.onNewClientConn(c)
}

func (h *gateHandler) OnClose(c kknet.IConn, err error) {
	//kklog.Debugf("[ccgate] client disconnected: connID=%d, remoteAddr=%s, err=%v", c.ID(), c.RemoteAddr(), err)
	h.gate.onClientConnClose(c)
}

// OnRaw 收到客户端消息，转发给逻辑节点
func (h *gateHandler) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	if data == nil || len(data.Bytes()) == 0 {
		return
	}
	defer kkbuffer.Put(data)

	sessionID := h.gate.clientMgr.getSessionByConnId(connID)
	if sessionID == "" {
		return // 客户端已断开|已被踢出会话
	}

	// 解析消息，应该路由到哪类逻辑服
	appOpts := h.gate.GetApplication().GetOptions()
	msgBytes, err := appOpts.StreamTool.MessageBytes(data.B)
	if err != nil {
		h.gate.feedCallback(connID, ERR_CLIENT_INVALID_PACKET)
		return
	}
	msgID, err := appOpts.ClientMsgPacket.GetMsgID(msgBytes)
	if err != nil {
		h.gate.feedCallback(connID, ERR_CLIENT_INVALID_PACKET)
		return
	}
	route, err := appOpts.ClientMsgPacket.GetRouter().GetMsgRoute(msgID)
	if err != nil {
		h.gate.feedCallback(connID, ERR_CLIENT_INVALID_PACKET)
		return
	}

	// 先为client选择一个逻辑服
	logicNode := h.gate.allocLogicNode(connID, route)
	if logicNode == nil {
		// 通知客户端分配逻辑服失败
		h.gate.feedCallback(connID, ERR_ALLOC_LOGIC_NODE_FAILED)
		return
	}

	// 将客户端消息原样转发给逻辑服
	streamBytes := data.B //transportor编码时是复制，所以这里可以直接传引用，不用再复制一次。
	if err := h.gate.transportor.ForwardToLogic(sessionID, streamBytes, logicNode.nodeId); err != nil {
		// 通知业务层，转发逻辑服失败。一般是逻辑服已断线或网络异常，直接当成服务器繁忙反馈。
		h.gate.feedCallback(connID, ERR_RECV_QUEUE_FULL)
	}
}
