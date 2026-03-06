package gametransrpc

import (
	"context"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/comps/ccgame/gametrans"
	"github.com/vvisun/kkdg/kkapp/comps/ptotrans"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/msgreceiver"
	"github.com/vvisun/kkdg/remotes/kkrpc"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

type transportorRpc struct {
	rpcClient   *kkrpc.Client
	sessionMgr  *gametrans.SessionManager
	msgReceiver *msgreceiver.MsgReceiver[string]
}

func NewTransportorRpc(sessionMgr *gametrans.SessionManager, msgReceiver *msgreceiver.MsgReceiver[string], node kkapp.INodeIdentity, rpcAddr string) gametrans.ITransportor {
	rpcRouter := kkrpc.NewRpcReceiver()
	rpcProcessor := &rpcHandler{}
	kkrpc.RegistOneWayHandler(rpcRouter, "register", rpcProcessor.onRegister)
	kkrpc.RegistOneWayHandler(rpcRouter, "s2c", rpcProcessor.onS2C)
	kkrpc.RegistOneWayHandler(rpcRouter, "s2cs", rpcProcessor.onS2Clients)
	kkrpc.RegistOneWayHandler(rpcRouter, "c2s", rpcProcessor.onC2S)

	rpcClient := kkrpc.NewClient(rpcAddr, kknet.DefaultOptions(), rpcRouter)
	if err := rpcClient.Start(); err != nil {
		kklog.Errorf("[ccgame] start rpc client error: %v", err)
		return nil
	}

	trans := &transportorRpc{
		sessionMgr:  sessionMgr,
		msgReceiver: msgReceiver,
		rpcClient:   rpcClient,
	}
	rpcProcessor.trans = trans

	// 注册到网关
	oneWayInvoker := kkrpc.NewOneWayInvoker[ptotrans.RpcMsgRegister](rpcClient, 0, "register")
	oneWayInvoker.InvokeNR(context.Background(), &ptotrans.RpcMsgRegister{
		NodeId:   node.GetNodeId(),
		NodeType: node.GetNodeType(),
	}, kkrpc.CallConfig{})

	return trans
}

func (slf *transportorRpc) ForwardToClient(sessionID string, msgBytes []byte) error {
	oneWayInvoker := kkrpc.NewOneWayInvoker[ptotrans.RpcS2Client](slf.rpcClient, 0, "s2c")
	oneWayInvoker.InvokeNR(context.Background(), &ptotrans.RpcS2Client{
		ClientId: sessionID,
		Payload:  msgBytes,
	}, kkrpc.CallConfig{})
	return nil
}

func (slf *transportorRpc) ForwardToClients(sessionIDs []string, msgBytes []byte) error {
	oneWayInvoker := kkrpc.NewOneWayInvoker[ptotrans.RpcS2Clients](slf.rpcClient, 0, "s2cs")
	oneWayInvoker.InvokeNR(context.Background(), &ptotrans.RpcS2Clients{
		ClientIds: sessionIDs,
		Payload:   msgBytes,
	}, kkrpc.CallConfig{})
	return nil
}

func (slf *transportorRpc) SendToClient(sessionID string, msg any) error {
	if sessionID == "" {
		return kkerrors.ErrEmptySessionID
	}
	if msg == nil {
		return kkerrors.ErrInvalidMessage
	}

	sessionInfo := slf.sessionMgr.GetSession(sessionID)
	if sessionInfo == nil {
		return kkerrors.ErrSessionNotFound
	}

	bb, err := kkpacket.EncodeStream(msg, kkpacket.DefaultStreamPacket(), kkapp.GetMsgPacket())
	if err != nil {
		kkbuffer.Put(bb)
		return err
	}

	streamBytes := bb.B

	oneWayInvoker := kkrpc.NewOneWayInvoker[ptotrans.RpcS2Client](slf.rpcClient, 0, "s2c")
	oneWayInvoker.InvokeNR(context.Background(), &ptotrans.RpcS2Client{
		ClientId: sessionID,
		Payload:  streamBytes,
	}, kkrpc.CallConfig{})
	kkbuffer.Put(bb)
	return nil
}

func (slf *transportorRpc) SendToClients(sessionIDs []string, msg any) error {
	if len(sessionIDs) == 0 {
		return nil
	}
	if msg == nil {
		return kkerrors.ErrInvalidMessage
	}

	bb, err := kkpacket.EncodeStream(msg, kkpacket.DefaultStreamPacket(), kkapp.GetMsgPacket())
	if err != nil {
		kkbuffer.Put(bb)
		return err
	}

	streamBytes := bb.B

	oneWayInvoker := kkrpc.NewOneWayInvoker[ptotrans.RpcS2Clients](slf.rpcClient, 0, "s2cs")
	oneWayInvoker.InvokeNR(context.Background(), &ptotrans.RpcS2Clients{
		ClientIds: sessionIDs,
		Payload:   streamBytes,
	}, kkrpc.CallConfig{})
	kkbuffer.Put(bb)
	return nil
}

//----------------------------------------------------------------

type rpcHandler struct {
	trans *transportorRpc
}

func (rh *rpcHandler) onRegister(ctx context.Context, msg *ptotrans.RpcMsgRegister, connId kknet.CONN_ID) error {
	return nil
}

func (rh *rpcHandler) onS2C(ctx context.Context, msg *ptotrans.RpcS2Client, connId kknet.CONN_ID) error {
	return nil
}

func (rh *rpcHandler) onS2Clients(ctx context.Context, msg *ptotrans.RpcS2Clients, connId kknet.CONN_ID) error {
	return nil
}

func (rh *rpcHandler) onC2S(ctx context.Context, msg *ptotrans.RpcC2S, connId kknet.CONN_ID) error {
	if rh.trans.sessionMgr.GetSession(msg.ClientId) == nil {
		rh.trans.sessionMgr.AddSession(msg.ClientId, msg.GateNodeId)
	}

	streamBytes := msg.Payload

	rh.trans.msgReceiver.OnSession(msg.ClientId, streamBytes)
	return nil
}
