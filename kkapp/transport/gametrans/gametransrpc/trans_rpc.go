package gametransrpc

import (
	"context"
	"time"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/transport/gametrans"
	"github.com/vvisun/kkdg/kkapp/transport/ptotrans"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/msgreceiver"
	"github.com/vvisun/kkdg/remotes/kkrpc"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kkcodec"
	"github.com/vvisun/kkdg/utils/kklog"
)

type transportorRpc struct {
	rpcClient   *kkrpc.Client
	sessionMgr  *gametrans.SessionManager
	msgReceiver *msgreceiver.MsgReceiver[string]
	stopped     bool
	nodeInfo    kkapp.INodeIdentity
}

var (
	onewayMsgRegister       kkrpc.OneWayInvoker[ptotrans.RpcMsgRegister]
	onewayS2Client          kkrpc.OneWayInvoker[ptotrans.RpcS2Client]
	onewayS2Clients         kkrpc.OneWayInvoker[ptotrans.RpcS2Clients]
	onewayC2S               kkrpc.OneWayInvoker[ptotrans.RpcC2S]
	onewayAllocClient       kkrpc.OneWayInvoker[ptotrans.RpcAllocClient]
	onewayClientDisconnect  kkrpc.OneWayInvoker[ptotrans.RpcClientDisconnect]
	onewayClientLoginLogout kkrpc.OneWayInvoker[ptotrans.RpcClientLoginLogout]
)

func NewTransportorRpc(sessionMgr *gametrans.SessionManager, msgReceiver *msgreceiver.MsgReceiver[string], node kkapp.INodeIdentity, rpcAddr string) (gametrans.ITransportor, error) {
	gStreamTool := kkpacket.NewLengthFieldStreamPacket(4, 4*1024)
	gFrameCodec := kkcodec.GetCodec(kkcodec.CodecTypeFlatBuffer)
	gPayloadCodec := kkcodec.GetCodec(kkcodec.CodecTypeMsgpack)
	methodMgr := kkrpc.NewMethodManager(gStreamTool, gFrameCodec, gPayloadCodec)
	ptotrans.InitRpcMsgs(methodMgr)
	rpcRouter := kkrpc.NewRpcReceiver(kkrpc.ApplyOptions(), methodMgr)
	rpcProcessor := &rpcHandler{}
	kkrpc.RegistOneWayHandler(rpcRouter, "c2s", rpcProcessor.onC2S)
	kkrpc.RegistOneWayHandler(rpcRouter, "clientDisconnect", rpcProcessor.onClientDisconnect)

	rpcClient := kkrpc.NewClient(rpcAddr, kknet.DefaultOptions(), rpcRouter)
	if err := rpcClient.Start(); err != nil {
		kklog.Errorf("[gametransrpc] 启动rpc客户端失败: %v", err)
		rpcClient.Stop()
		return nil, err
	}

	onewayMsgRegister, _ = kkrpc.NewOneWayInvoker[ptotrans.RpcMsgRegister](rpcClient, 0)
	onewayS2Client, _ = kkrpc.NewOneWayInvoker[ptotrans.RpcS2Client](rpcClient, 0)
	onewayS2Clients, _ = kkrpc.NewOneWayInvoker[ptotrans.RpcS2Clients](rpcClient, 0)
	onewayC2S, _ = kkrpc.NewOneWayInvoker[ptotrans.RpcC2S](rpcClient, 0)
	onewayAllocClient, _ = kkrpc.NewOneWayInvoker[ptotrans.RpcAllocClient](rpcClient, 0)
	onewayClientDisconnect, _ = kkrpc.NewOneWayInvoker[ptotrans.RpcClientDisconnect](rpcClient, 0)
	onewayClientLoginLogout, _ = kkrpc.NewOneWayInvoker[ptotrans.RpcClientLoginLogout](rpcClient, 0)

	trans := &transportorRpc{
		sessionMgr:  sessionMgr,
		msgReceiver: msgReceiver,
		rpcClient:   rpcClient,
		nodeInfo:    node,
	}
	rpcProcessor.trans = trans
	msgReceiver.SetNeedCopyInOnSession(true)

	// 注册到网关
	trans.registerToGateway(node)

	return trans, nil
}

func (slf *transportorRpc) Stop() error {
	if slf.stopped {
		return nil
	}
	slf.stopped = true
	return slf.rpcClient.Stop()
}

func (slf *transportorRpc) registerToGateway(node kkapp.INodeIdentity) {
	go func() {
		//循环注册到网关，直到成功为止
		for {
			if slf.stopped {
				kklog.Warnf("[gametransrpc] rpc client stopped, stop register to gateway loop")
				return
			}
			err := onewayMsgRegister.InvokeNR(context.Background(), &ptotrans.RpcMsgRegister{
				NodeId:   node.GetNodeId(),
				NodeType: node.GetNodeType(),
			}, kkrpc.CallConfig{})
			if err == nil {
				kklog.Infof("[gametransrpc] 注册到网关成功")
				return
			}
			kklog.Warnf("[gametransrpc] 注册到网关失败, 重试中...")
			time.Sleep(1 * time.Second)
		}
	}()
}

// @param packet is a full stream packet [length,message]
func (slf *transportorRpc) ForwardToClient(sessionID string, packet []byte) error {
	if slf.stopped {
		return kkerrors.ErrAppTransportorStopped
	}
	if len(packet) == 0 {
		return kkerrors.ErrAppEmptyMsgBytes
	}
	err := onewayS2Client.InvokeNR(context.Background(), &ptotrans.RpcS2Client{
		ClientId: sessionID,
		Payload:  packet,
	}, kkrpc.CallConfig{})
	if err != nil {
		return err
	}
	return nil
}

// @param packet is a full stream packet [length,message]
func (slf *transportorRpc) ForwardToClients(sessionIDs []string, packet []byte) error {
	if slf.stopped {
		return kkerrors.ErrAppTransportorStopped
	}
	if len(packet) == 0 {
		return kkerrors.ErrAppEmptyMsgBytes
	}
	err := onewayS2Clients.InvokeNR(context.Background(), &ptotrans.RpcS2Clients{
		ClientIds: sessionIDs,
		Payload:   packet,
	}, kkrpc.CallConfig{})
	if err != nil {
		return err
	}
	return nil
}

func (slf *transportorRpc) SendToClient(sessionID string, msg any) error {
	if slf.stopped {
		return kkerrors.ErrAppTransportorStopped
	}
	if sessionID == "" {
		return kkerrors.ErrAppEmptySessionID
	}
	if msg == nil {
		return kkerrors.ErrPktInvalidMessage
	}

	sessionInfo := slf.sessionMgr.GetSession(sessionID)
	if sessionInfo == nil {
		return kkerrors.ErrAppSessionNotFound
	}

	bb, err := kkpacket.EncodeStream(msg, kkapp.GetStreamTool(), kkapp.GetMsgPacket())
	if err != nil {
		kkbuffer.Put(bb)
		return err
	}

	streamBytes := bb.B

	err = onewayS2Client.InvokeNR(context.Background(), &ptotrans.RpcS2Client{
		ClientId: sessionID,
		Payload:  streamBytes,
	}, kkrpc.CallConfig{})
	kkbuffer.Put(bb)
	if err != nil {
		return err
	}
	return nil
}

func (slf *transportorRpc) SendToClients(sessionIDs []string, msg any) error {
	if slf.stopped {
		return kkerrors.ErrAppTransportorStopped
	}
	if len(sessionIDs) == 0 {
		return nil
	}
	if msg == nil {
		return kkerrors.ErrPktInvalidMessage
	}

	bb, err := kkpacket.EncodeStream(msg, kkapp.GetStreamTool(), kkapp.GetMsgPacket())
	if err != nil {
		kkbuffer.Put(bb)
		return err
	}

	streamBytes := bb.B

	err = onewayS2Clients.InvokeNR(context.Background(), &ptotrans.RpcS2Clients{
		ClientIds: sessionIDs,
		Payload:   streamBytes,
	}, kkrpc.CallConfig{})
	kkbuffer.Put(bb)
	if err != nil {
		return err
	}
	return nil
}

func (slf *transportorRpc) NotifyClientLoginLogout(sessionID string, userId int64, isLogin bool) error {
	if slf.stopped {
		return kkerrors.ErrAppTransportorStopped
	}
	if sessionID == "" {
		return nil
	}
	sessionInfo := slf.sessionMgr.GetSession(sessionID)
	if sessionInfo == nil {
		return kkerrors.ErrAppSessionNotFound
	}
	var msg ptotrans.RpcClientLoginLogout
	msg.ClientId = sessionID
	msg.UserId = userId
	msg.IsLogin = isLogin
	msg.NodeType = slf.nodeInfo.GetNodeType()
	msg.NodeId = slf.nodeInfo.GetNodeId()
	msg.GateNodeId = sessionInfo.GetGateNodeID()
	return onewayClientLoginLogout.InvokeNR(context.Background(), &msg, kkrpc.CallConfig{})
}

//----------------------------------------------------------------

type rpcHandler struct {
	trans *transportorRpc
}

// 网关转发客户端消息到逻辑服: 客户端->网关->逻辑服
func (rh *rpcHandler) onC2S(ctx context.Context, msg *ptotrans.RpcC2S, connId kknet.CONN_ID) error {
	if rh.trans.sessionMgr.GetSession(msg.ClientId) == nil {
		rh.trans.sessionMgr.AddSession(msg.ClientId, msg.GateNodeId)
	}

	streamBytes := msg.Payload

	rh.trans.msgReceiver.OnSession(msg.ClientId, streamBytes)
	return nil
}

// 网关转发客户端断开事件到逻辑服: 客户端->网关->逻辑服
func (rh *rpcHandler) onClientDisconnect(ctx context.Context, msg *ptotrans.RpcClientDisconnect, connId kknet.CONN_ID) error {
	rh.trans.sessionMgr.RemoveSession(msg.ClientId)
	for _, clientId := range msg.ClientIds {
		rh.trans.sessionMgr.RemoveSession(clientId)
	}
	kklog.Debugf("[gametransrpc] 客户端断开 clientId=%s clientIds=%v", msg.ClientId, msg.ClientIds)
	return nil
}
