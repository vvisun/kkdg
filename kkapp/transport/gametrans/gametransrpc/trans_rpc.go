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
}

var (
	onewayMsgRegister      kkrpc.OneWayInvoker[ptotrans.RpcMsgRegister]
	onewayS2Client         kkrpc.OneWayInvoker[ptotrans.RpcS2Client]
	onewayS2Clients        kkrpc.OneWayInvoker[ptotrans.RpcS2Clients]
	onewayC2S              kkrpc.OneWayInvoker[ptotrans.RpcC2S]
	onewayClientDisconnect kkrpc.OneWayInvoker[ptotrans.RpcClientDisconnect]
)

func NewTransportorRpc(sessionMgr *gametrans.SessionManager, msgReceiver *msgreceiver.MsgReceiver[string], node kkapp.INodeIdentity, rpcAddr string) (gametrans.ITransportor, error) {
	gStreamTool := kkpacket.NewLengthFieldStreamPacket(4, 4*1024)
	gFrameCodec := kkcodec.GetCodec(kkcodec.CodecTypeFlatBuffer)
	gPayloadCodec := kkcodec.GetCodec(kkcodec.CodecTypeMsgpack)
	rpcRouter := kkrpc.NewRpcReceiver(gStreamTool, gFrameCodec, gPayloadCodec)
	rpcProcessor := &rpcHandler{}
	kkrpc.RegistOneWayHandler(rpcRouter, "register", rpcProcessor.onRegister)
	kkrpc.RegistOneWayHandler(rpcRouter, "s2c", rpcProcessor.onS2C)
	kkrpc.RegistOneWayHandler(rpcRouter, "s2cs", rpcProcessor.onS2Clients)
	kkrpc.RegistOneWayHandler(rpcRouter, "c2s", rpcProcessor.onC2S)

	rpcClient := kkrpc.NewClient(rpcAddr, kknet.DefaultOptions(), rpcRouter)
	if err := rpcClient.Start(); err != nil {
		kklog.Errorf("[ccgame] start rpc client error: %v", err)
		rpcClient.Stop()
		return nil, err
	}

	onewayMsgRegister, _ = kkrpc.NewOneWayInvoker[ptotrans.RpcMsgRegister](rpcClient, 0)
	onewayS2Client, _ = kkrpc.NewOneWayInvoker[ptotrans.RpcS2Client](rpcClient, 0)
	onewayS2Clients, _ = kkrpc.NewOneWayInvoker[ptotrans.RpcS2Clients](rpcClient, 0)
	onewayC2S, _ = kkrpc.NewOneWayInvoker[ptotrans.RpcC2S](rpcClient, 0)
	onewayClientDisconnect, _ = kkrpc.NewOneWayInvoker[ptotrans.RpcClientDisconnect](rpcClient, 0)

	trans := &transportorRpc{
		sessionMgr:  sessionMgr,
		msgReceiver: msgReceiver,
		rpcClient:   rpcClient,
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
				kklog.Warnf("[ccgame] rpc client stopped, stop register to gateway loop")
				return
			}
			err := onewayMsgRegister.InvokeNR(context.Background(), &ptotrans.RpcMsgRegister{
				NodeId:   node.GetNodeId(),
				NodeType: node.GetNodeType(),
			}, kkrpc.CallConfig{})
			if err == nil {
				kklog.Infof("[ccgame] register to gateway success")
				return
			}
			kklog.Warnf("[ccgame] register to gateway failed, retrying...")
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
