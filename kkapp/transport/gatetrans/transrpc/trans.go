package transrpc

import (
	"context"

	"github.com/vvisun/kkdg/kkapp/transport/gatetrans"
	"github.com/vvisun/kkdg/kkapp/transport/ptotrans"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/remotes/kkrpc"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kkcodec"
	"github.com/vvisun/kkdg/utils/kklog"
)

// transportorRpc 使用RPC转发消息
// 逻辑服先连接到本网关, 然后发送[register:nodeId,nodeType]注册到本网关, 进行注册服务。
type transportorRpc struct {
	rpcSvr       *kkrpc.Server
	sessionMgr   gatetrans.ISessionManager
	logicNodeMgr *logicNodeMgr
	gateNodeId   string
	stopped      bool
	msgHooker    *gatetrans.MsgHooker
}

var (
	onewayMsgRegister       kkrpc.OneWayInvoker[ptotrans.RpcMsgRegister]
	onewayS2C               kkrpc.OneWayInvoker[ptotrans.RpcS2Client]
	onewayS2Clients         kkrpc.OneWayInvoker[ptotrans.RpcS2Clients]
	onewayC2S               kkrpc.OneWayInvoker[ptotrans.RpcC2S]
	onewayClientDisconnect  kkrpc.OneWayInvoker[ptotrans.RpcClientDisconnect]
	onewayAllocClient       kkrpc.OneWayInvoker[ptotrans.RpcAllocClient]
	onewayClientLoginLogout kkrpc.OneWayInvoker[ptotrans.RpcClientLoginLogout]
	onewayUnregister        kkrpc.OneWayInvoker[ptotrans.RpcUnregister]
)

var _ gatetrans.ITransportor = (*transportorRpc)(nil)
var _ gatetrans.IMemberMgrGetter = (*transportorRpc)(nil)

func NewTransportorRpc(sessionMgr gatetrans.ISessionManager, gateNodeId string, rpcAddr string) (gatetrans.ITransportor, error) {
	gStreamTool := kkpacket.NewLengthFieldStreamPacket(4, 4*1024)
	gFrameCodec := kkcodec.GetCodec(kkcodec.CodecTypeFlatBuffer)
	gPayloadCodec := kkcodec.GetCodec(kkcodec.CodecTypeMsgpack)
	methodMgr := kkrpc.NewMethodManager(gStreamTool, gFrameCodec, gPayloadCodec)
	ptotrans.InitRpcMsgs(methodMgr)
	rpcRouter := kkrpc.NewRpcReceiver(kkrpc.ApplyOptions(), methodMgr)
	rpcProcessor := &rpcHandler{}
	kkrpc.RegistOneWayHandler(rpcRouter, rpcProcessor.onRegister)
	kkrpc.RegistOneWayHandler(rpcRouter, rpcProcessor.onS2C)
	kkrpc.RegistOneWayHandler(rpcRouter, rpcProcessor.onS2Clients)
	kkrpc.RegistOneWayHandler(rpcRouter, rpcProcessor.onC2S)
	kkrpc.RegistOneWayHandler(rpcRouter, rpcProcessor.onClientLoginLogout)
	kkrpc.RegistOneWayHandler(rpcRouter, rpcProcessor.onUnregister)

	rpcSvr := kkrpc.NewServer(rpcAddr, kknet.DefaultOptions(), rpcRouter)
	if err := rpcSvr.Start(); err != nil {
		kklog.Errorf("[transrpc] 启动rpc服务器失败: %v", err)
		return nil, err
	}

	onewayMsgRegister, _ = kkrpc.NewOneWayInvoker[ptotrans.RpcMsgRegister](rpcSvr)
	onewayS2C, _ = kkrpc.NewOneWayInvoker[ptotrans.RpcS2Client](rpcSvr)
	onewayS2Clients, _ = kkrpc.NewOneWayInvoker[ptotrans.RpcS2Clients](rpcSvr)
	onewayC2S, _ = kkrpc.NewOneWayInvoker[ptotrans.RpcC2S](rpcSvr)
	onewayClientDisconnect, _ = kkrpc.NewOneWayInvoker[ptotrans.RpcClientDisconnect](rpcSvr)
	onewayAllocClient, _ = kkrpc.NewOneWayInvoker[ptotrans.RpcAllocClient](rpcSvr)
	onewayClientLoginLogout, _ = kkrpc.NewOneWayInvoker[ptotrans.RpcClientLoginLogout](rpcSvr)
	onewayUnregister, _ = kkrpc.NewOneWayInvoker[ptotrans.RpcUnregister](rpcSvr)

	trans := &transportorRpc{
		sessionMgr:   sessionMgr,
		logicNodeMgr: newLogicNodeMgr(),
		gateNodeId:   gateNodeId,
		rpcSvr:       rpcSvr,
		msgHooker:    gatetrans.NewMsgHooker(),
	}
	rpcProcessor.trans = trans
	rpcSvr.SetLifeCycleHandler(trans)

	return trans, nil
}

func (slf *transportorRpc) Stop() error {
	if slf.stopped {
		return nil
	}
	slf.stopped = true
	return slf.rpcSvr.Stop()
}

func (slf *transportorRpc) OnConnect(conn kknet.IConn) {
	kklog.Infof("[transrpc] rpc服务器新连接... connId=%d", conn.ID())
}

func (slf *transportorRpc) OnClose(conn kknet.IConn, err error) {
	kklog.Infof("[transrpc] rpc服务器连接关闭... connId=%d, err=%v", conn.ID(), err)
}

func (slf *transportorRpc) ForwardToLogic(sessionID string, msgBytes []byte, logicNodeId string) error {
	if slf.stopped {
		return kkerrors.ErrAppTransportorStopped
	}
	if len(msgBytes) == 0 {
		return kkerrors.ErrAppEmptyMsgBytes
	}
	memberInfo := slf.logicNodeMgr.getLogicNode(logicNodeId)
	if memberInfo == nil {
		return ErrLogicNodeNotRegistered //逻辑节点未注册
	}
	_, err := slf.sessionMgr.GetConn(sessionID)
	if err != nil {
		return err //客户端已下线
	}

	// 这里无需复制，因为InvokeNR会编码自动复制一次。
	streamBytes := msgBytes

	err = onewayC2S.InvokeNR(context.Background(), &ptotrans.RpcC2S{
		ClientId:   sessionID,
		GateNodeId: slf.gateNodeId,
		Payload:    streamBytes,
	}, kkrpc.CallConfig{ConnId: memberInfo.connId})
	if err != nil {
		return err
	}
	return nil
}

// @param packet is a full stream packet [length,message]
func (slf *transportorRpc) ForwardToClient(sessionID string, packet []byte) error {
	if slf.stopped {
		return kkerrors.ErrAppTransportorStopped
	}
	if sessionID == "" {
		return kkerrors.ErrAppEmptySessionID
	}
	if len(packet) == 0 {
		return kkerrors.ErrAppEmptyMsgBytes
	}

	conn, err := slf.sessionMgr.GetConn(sessionID)
	if err != nil {
		return err //客户端已下线
	}

	// 这里需要复制，因为传递过来的msgBytes可能会被其他地方回收修改。
	streamBytes := kkbuffer.GetWithCapacity(len(packet))
	streamBytes.WriteBytes(packet)

	if err := conn.SendBuffer(streamBytes); err != nil {
		kklog.Debugf("[transrpc] 发送响应失败: %v", err)
		return err
	}
	return nil
}

// @param packet is a full stream packet [length,message]
func (slf *transportorRpc) ForwardToClients(sessionIDs []string, packet []byte) error {
	if slf.stopped {
		return kkerrors.ErrAppTransportorStopped
	}
	if len(sessionIDs) == 0 {
		return nil
	}
	if len(packet) == 0 {
		return kkerrors.ErrAppEmptyMsgBytes
	}

	var loopErr error
	for _, sessionID := range sessionIDs {
		if sessionID == "" {
			continue
		}

		conn, err := slf.sessionMgr.GetConn(sessionID)
		if err != nil {
			continue //客户端已下线
		}

		// 这里需要复制，因为传递过来的msgBytes可能会被其他地方回收修改。
		// 而且SendBuffer会自动释放streamBytes。所以需要复制一份。
		streamBytes := kkbuffer.GetWithCapacity(len(packet))
		streamBytes.WriteBytes(packet)

		if err := conn.SendBuffer(streamBytes); err != nil {
			kklog.Debugf("[transrpc] 发送响应失败: %v", err)
			loopErr = err
		}
	}
	return loopErr
}

func (slf *transportorRpc) NotifyClientDisconnect(sessionID string, logicNodeId string, connId kknet.CONN_ID) error {
	if slf.stopped {
		return kkerrors.ErrAppTransportorStopped
	}
	memberInfo := slf.logicNodeMgr.getLogicNode(logicNodeId)
	if memberInfo == nil {
		return ErrLogicNodeNotRegistered //逻辑节点未注册
	}
	err := onewayClientDisconnect.InvokeNR(context.Background(), &ptotrans.RpcClientDisconnect{
		ClientId: sessionID,
	}, kkrpc.CallConfig{ConnId: memberInfo.connId})
	if err != nil {
		return err
	}
	return nil
}

func (slf *transportorRpc) NotifyClientConnect(sessionID string, logicNodeId string, connId kknet.CONN_ID) error {
	if slf.stopped {
		return kkerrors.ErrAppTransportorStopped
	}
	memberInfo := slf.logicNodeMgr.getLogicNode(logicNodeId)
	if memberInfo == nil {
		return ErrLogicNodeNotRegistered //逻辑节点未注册
	}
	err := onewayAllocClient.InvokeNR(context.Background(), &ptotrans.RpcAllocClient{
		ClientId: sessionID,
	}, kkrpc.CallConfig{ConnId: memberInfo.connId})
	if err != nil {
		return err
	}
	return nil
}

func (slf *transportorRpc) GetMemberMgr() gatetrans.IMemberMgr {
	return slf.logicNodeMgr
}

func (slf *transportorRpc) HookMsg(listener gatetrans.MsgHookListener) {
	slf.msgHooker.AddListener(listener)
}
