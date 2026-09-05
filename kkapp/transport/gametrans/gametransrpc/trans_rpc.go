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
	"github.com/vvisun/kkdg/remotes/kkrpc"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kkcodec"
	"github.com/vvisun/kkdg/utils/kklog"
)

type transportorRpc struct {
	rpcClient        *kkrpc.Client
	sessionMgr       *gametrans.SessionManager
	msgReceiver      gametrans.ISessionMsgReceiver
	stopped          bool
	nodeInfo         kkapp.INodeIdentity
	clientMsgPacket  *kkpacket.MessagePacket
	clientStreamTool kkpacket.IPacket
	invokers         *onewayInvokers
}

type onewayInvokers struct {
	onewayMsgRegister       kkrpc.OneWayInvoker[ptotrans.RpcMsgRegister]
	onewayS2Client          kkrpc.OneWayInvoker[ptotrans.RpcS2Client]
	onewayS2Clients         kkrpc.OneWayInvoker[ptotrans.RpcS2Clients]
	onewayC2S               kkrpc.OneWayInvoker[ptotrans.RpcC2S]
	onewayAllocClient       kkrpc.OneWayInvoker[ptotrans.RpcAllocClient]
	onewayClientDisconnect  kkrpc.OneWayInvoker[ptotrans.RpcClientDisconnect]
	onewayClientLoginLogout kkrpc.OneWayInvoker[ptotrans.RpcClientLoginLogout]
	onewayUnregister        kkrpc.OneWayInvoker[ptotrans.RpcUnregister]
	onewayCloseClient       kkrpc.OneWayInvoker[ptotrans.RpcCloseClient]
}

func newOnewayInvokers(rpcClient *kkrpc.Client) (*onewayInvokers, error) {
	invokers := &onewayInvokers{}

	var err error
	if invokers.onewayMsgRegister, err = kkrpc.NewOneWayInvoker[ptotrans.RpcMsgRegister](rpcClient); err != nil {
		return nil, err
	}
	if invokers.onewayS2Client, err = kkrpc.NewOneWayInvoker[ptotrans.RpcS2Client](rpcClient); err != nil {
		return nil, err
	}
	if invokers.onewayS2Clients, err = kkrpc.NewOneWayInvoker[ptotrans.RpcS2Clients](rpcClient); err != nil {
		return nil, err
	}
	if invokers.onewayC2S, err = kkrpc.NewOneWayInvoker[ptotrans.RpcC2S](rpcClient); err != nil {
		return nil, err
	}
	if invokers.onewayAllocClient, err = kkrpc.NewOneWayInvoker[ptotrans.RpcAllocClient](rpcClient); err != nil {
		return nil, err
	}
	if invokers.onewayClientDisconnect, err = kkrpc.NewOneWayInvoker[ptotrans.RpcClientDisconnect](rpcClient); err != nil {
		return nil, err
	}
	if invokers.onewayClientLoginLogout, err = kkrpc.NewOneWayInvoker[ptotrans.RpcClientLoginLogout](rpcClient); err != nil {
		return nil, err
	}
	if invokers.onewayUnregister, err = kkrpc.NewOneWayInvoker[ptotrans.RpcUnregister](rpcClient); err != nil {
		return nil, err
	}
	if invokers.onewayCloseClient, err = kkrpc.NewOneWayInvoker[ptotrans.RpcCloseClient](rpcClient); err != nil {
		return nil, err
	}

	return invokers, nil
}

func NewTransportorRpc(
	sessionMgr *gametrans.SessionManager,
	msgReceiver gametrans.ISessionMsgReceiver,
	node kkapp.INodeIdentity,
	rpcAddr string,
	clientMsgPacket *kkpacket.MessagePacket,
	clientStreamTool kkpacket.IPacket,
) (gametrans.ITransportor, error) {
	if err := gametrans.CheckReceiverWorkers(sessionMgr, msgReceiver); err != nil {
		return nil, err
	}
	gStreamTool := kkpacket.NewLengthFieldStreamPacket(4, 4*1024)
	gFrameCodec := kkcodec.GetCodec(kkcodec.CodecTypeJson)
	gPayloadCodec := kkcodec.GetCodec(kkcodec.CodecTypeJson)
	methodMgr := kkrpc.NewMethodManager(gStreamTool, gFrameCodec, gPayloadCodec)
	ptotrans.InitRpcMsgs(methodMgr)
	rpcRouter := kkrpc.NewRpcReceiver(kkrpc.ApplyOptions(), methodMgr)

	rpcClient := kkrpc.NewClient(rpcAddr, kknet.DefaultOptions(), rpcRouter)
	if err := rpcClient.Start(); err != nil {
		kklog.Errorf("[gametransrpc] 启动rpc客户端失败: %v", err)
		rpcClient.Stop()
		return nil, err
	}

	invokers, err := newOnewayInvokers(rpcClient)
	if err != nil {
		return nil, err
	}

	trans := &transportorRpc{
		sessionMgr:       sessionMgr,
		msgReceiver:      msgReceiver,
		rpcClient:        rpcClient,
		nodeInfo:         node,
		clientMsgPacket:  clientMsgPacket,
		clientStreamTool: clientStreamTool,
		invokers:         invokers,
	}
	rpcProcessor := &rpcHandler{
		trans: trans,
	}
	kkrpc.RegistOneWayHandler(rpcRouter, rpcProcessor.onC2S)
	kkrpc.RegistOneWayHandler(rpcRouter, rpcProcessor.onClientDisconnect)
	kkrpc.RegistOneWayHandler(rpcRouter, rpcProcessor.onAllocClient)

	// 注册到网关
	trans.registerToGateway(node)

	return trans, nil
}

func (slf *transportorRpc) Stop() error {
	if slf.stopped {
		return nil
	}

	slf.invokers.onewayUnregister.InvokeNR(context.Background(), &ptotrans.RpcUnregister{
		NodeId: slf.nodeInfo.GetNodeId(),
	}, kkrpc.CallConfig{})

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
			err := slf.invokers.onewayMsgRegister.InvokeNR(context.Background(), &ptotrans.RpcMsgRegister{
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
	if sessionID == "" {
		return kkerrors.ErrAppEmptySessionID
	}
	if len(packet) == 0 {
		return kkerrors.ErrAppEmptyMsgBytes
	}
	if slf.sessionMgr.GetSession(sessionID) == nil {
		return kkerrors.ErrAppSessionNotFound
	}
	err := slf.invokers.onewayS2Client.InvokeNR(context.Background(), &ptotrans.RpcS2Client{
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
	if len(sessionIDs) == 0 {
		return nil
	}
	if len(packet) == 0 {
		return kkerrors.ErrAppEmptyMsgBytes
	}
	if len(sessionIDs) == 1 {
		return slf.ForwardToClient(sessionIDs[0], packet)
	}
	err := slf.invokers.onewayS2Clients.InvokeNR(context.Background(), &ptotrans.RpcS2Clients{
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

	bb, err := kkpacket.EncodeStream(msg, slf.clientStreamTool, slf.clientMsgPacket)
	if err != nil {
		kkbuffer.Put(bb)
		return err
	}

	streamBytes := bb.B

	err = slf.invokers.onewayS2Client.InvokeNR(context.Background(), &ptotrans.RpcS2Client{
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

	bb, err := kkpacket.EncodeStream(msg, slf.clientStreamTool, slf.clientMsgPacket)
	if err != nil {
		kkbuffer.Put(bb)
		return err
	}

	streamBytes := bb.B

	err = slf.invokers.onewayS2Clients.InvokeNR(context.Background(), &ptotrans.RpcS2Clients{
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
	return slf.invokers.onewayClientLoginLogout.InvokeNR(context.Background(), &msg, kkrpc.CallConfig{})
}

func (slf *transportorRpc) CloseClient(sessionID string, reason string) error {
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
	return slf.invokers.onewayCloseClient.InvokeNR(context.Background(), &ptotrans.RpcCloseClient{
		ClientId:   sessionID,
		Reason:     reason,
		GateNodeId: sessionInfo.GetGateNodeID(),
	}, kkrpc.CallConfig{})
}

func (slf *transportorRpc) GetSessionManager() *gametrans.SessionManager {
	return slf.sessionMgr
}

//----------------------------------------------------------------

type rpcHandler struct {
	trans *transportorRpc
}

// 网关转发客户端消息到逻辑服: 客户端->网关->逻辑服
func (rh *rpcHandler) onC2S(ctx context.Context, msg *ptotrans.RpcC2S, connId kknet.CONN_ID) error {
	sessionInfo := rh.trans.sessionMgr.GetSession(msg.ClientId)
	if sessionInfo == nil {
		sessionInfo = rh.trans.sessionMgr.AddSession(msg.ClientId, msg.GateNodeId)
	}

	streamBytes := msg.Payload

	rh.trans.msgReceiver.OnSession(msg.ClientId, streamBytes, sessionInfo.GetThreadIdx())
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

// 分配客户端到本逻辑服
func (rh *rpcHandler) onAllocClient(ctx context.Context, msg *ptotrans.RpcAllocClient, connId kknet.CONN_ID) error {
	// 这里可以不处理，因为在onC2S里会添加到sessionMgr中
	// rh.trans.sessionMgr.AddSession(msg.ClientId, msg.GateNodeId)
	kklog.Debugf("[gametransrpc] 分配客户端到本逻辑服 clientId=%s", msg.ClientId)
	return nil
}
