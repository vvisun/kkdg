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
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkgws"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/remotes/kkcluster"
	"github.com/vvisun/kkdg/remotes/kkcluster/cnats"
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
	"github.com/vvisun/kkdg/remotes/kkdiscovery/dnats"
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
	if slf.gateOpt.DiscoveryOpts.Url != "" {
		slf.discovery = dnats.NewNatsDiscovery(
			slf.GetApplication().GetNodeInfo(),
			slf.gateOpt.DiscoveryOpts,
		)
	}

	// 初始化 cluster（用于 gate <-> logic 转发）
	if slf.gateOpt.ClusterOpts.Url != "" {
		clusterOpts := slf.gateOpt.ClusterOpts
		if slf.discovery != nil {
			kkoption.ApplyOptionsTo(&clusterOpts,
				kkcluster.WithDiscovery(slf.discovery),
			)
		}
		slf.cluster = cnats.NewNatsCluster(
			slf.GetApplication().GetNodeId(),
			slf.GetApplication().GetNodeType(),
			clusterOpts,
		)
	}

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
	// 启动 discovery
	if slf.discovery != nil {
		if err := slf.discovery.Start(); err != nil {
			return err
		}
	}

	// 启动 cluster
	if slf.cluster != nil {
		if err := slf.cluster.Start(); err != nil {
			return err
		}
	}

	// 启动客户端监听服务器（WebSocket 或 TCP）
	if slf.gateOpt.WSAddr != "" {
		if err := slf.startWSServer(); err != nil {
			return err
		}
	} else if slf.gateOpt.TCPAddr != "" {
		if err := slf.startTCPServer(); err != nil {
			return err
		}
	}

	return nil
}

func (slf *gateComponent) OnStop() error {
	// 停止客户端监听服务器（WebSocket 或 TCP）
	if slf.server != nil {
		if err := slf.server.Stop(); err != nil {
			kklog.Errorf("[ccgate] stop tcp server error: %v", err)
		}
	}

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
