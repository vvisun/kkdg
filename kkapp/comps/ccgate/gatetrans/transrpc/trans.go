package transrpc

import (
	"context"

	"github.com/vvisun/kkdg/kkapp/comps/ccgate/gatetrans"
	"github.com/vvisun/kkdg/kkapp/comps/ptotrans"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
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
}

var _ gatetrans.ITransportor = (*transportorRpc)(nil)

func NewTransportorRpc(sessionMgr gatetrans.ISessionManager, gateNodeId string, rpcAddr string) (gatetrans.ITransportor, error) {
	gFrameCodec := kkcodec.GetCodec(kkcodec.CodecTypeFlatBuffer)
	gPayloadCodec := kkcodec.GetCodec(kkcodec.CodecTypeMsgpack)
	rpcRouter := kkrpc.NewRpcReceiver(gFrameCodec, gPayloadCodec)
	rpcProcessor := &rpcHandler{}
	kkrpc.RegistOneWayHandler(rpcRouter, "register", rpcProcessor.onRegister)
	kkrpc.RegistOneWayHandler(rpcRouter, "s2c", rpcProcessor.onS2C)
	kkrpc.RegistOneWayHandler(rpcRouter, "s2cs", rpcProcessor.onS2Clients)
	kkrpc.RegistOneWayHandler(rpcRouter, "c2s", rpcProcessor.onC2S)

	rpcSvr := kkrpc.NewServer(rpcAddr, kknet.DefaultOptions(), rpcRouter)
	if err := rpcSvr.Start(); err != nil {
		kklog.Errorf("[ccgate] start rpc server error: %v", err)
		return nil, err
	}

	trans := &transportorRpc{
		sessionMgr:   sessionMgr,
		logicNodeMgr: &logicNodeMgr{},
		gateNodeId:   gateNodeId,
		rpcSvr:       rpcSvr,
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
	kklog.Infof("[ccgate] rpc server new connection... connId=%d", conn.ID())
}

func (slf *transportorRpc) OnClose(conn kknet.IConn, err error) {
	kklog.Infof("[ccgate] rpc server connection closed... connId=%d, err=%v", conn.ID(), err)
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

	oneWayInvoker, err := kkrpc.NewOneWayInvoker[ptotrans.RpcC2S](slf.rpcSvr, memberInfo.connId)
	if err != nil {
		return err
	}
	err = oneWayInvoker.InvokeNR(context.Background(), &ptotrans.RpcC2S{
		ClientId:   sessionID,
		GateNodeId: slf.gateNodeId,
		Payload:    streamBytes,
	}, kkrpc.CallConfig{})
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
		kklog.Errorf("[ccgate] send response error: %v", err)
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
			kklog.Errorf("[ccgate] send response error: %v", err)
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
	oneWayInvoker, err := kkrpc.NewOneWayInvoker[ptotrans.RpcClientDisconnect](slf.rpcSvr, memberInfo.connId)
	if err != nil {
		return err
	}
	err = oneWayInvoker.InvokeNR(context.Background(), &ptotrans.RpcClientDisconnect{
		ClientId: sessionID,
	}, kkrpc.CallConfig{})
	if err != nil {
		return err
	}
	return nil
}
