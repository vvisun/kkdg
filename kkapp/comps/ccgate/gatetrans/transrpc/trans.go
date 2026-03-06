package transrpc

import (
	"context"
	"sync"

	"github.com/vvisun/kkdg/kkapp/comps/ccgate/gatetrans"
	"github.com/vvisun/kkdg/kkapp/comps/ptotrans"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/remotes/kkrpc"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

type logicMemberInfo struct {
	nodeId   string
	nodeType string
	connId   kknet.CONN_ID
}

type logicNodeMgr struct {
	logicNodeMap sync.Map // map[nodeId]*logicMemberInfo
	connMap      sync.Map // map[connId]nodeId
}

func (slf *logicNodeMgr) registerLogicNode(nodeId string, nodeType string, connId kknet.CONN_ID) {
	memberInfo := &logicMemberInfo{
		nodeId:   nodeId,
		nodeType: nodeType,
		connId:   connId,
	}
	slf.logicNodeMap.Store(nodeId, memberInfo)
	slf.connMap.Store(connId, nodeId)
}

func (slf *logicNodeMgr) unregisterLogicNode(nodeId string) {
	info, ok := slf.logicNodeMap.Load(nodeId)
	if !ok {
		return
	}
	memberInfo := info.(*logicMemberInfo)
	slf.connMap.Delete(memberInfo.connId)
	slf.logicNodeMap.Delete(nodeId)
}

func (slf *logicNodeMgr) getLogicNode(nodeId string) *logicMemberInfo {
	value, ok := slf.logicNodeMap.Load(nodeId)
	if !ok {
		return nil
	}
	return value.(*logicMemberInfo)
}

func (slf *logicNodeMgr) getLogicNodeByConnId(connId kknet.CONN_ID) *logicMemberInfo {
	value, ok := slf.connMap.Load(connId)
	if !ok {
		return nil
	}
	return value.(*logicMemberInfo)
}

// transportorRpc 使用RPC转发消息
// 逻辑服先连接到本网关, 然后发送[register:nodeId,nodeType]注册到本网关, 进行注册服务。
type transportorRpc struct {
	rpcSvr       *kkrpc.Server
	sessionMgr   gatetrans.ISessionManager
	logicNodeMgr *logicNodeMgr
	gateNodeId   string
}

var _ gatetrans.ITransportor = (*transportorRpc)(nil)

func NewTransportorRpc(sessionMgr gatetrans.ISessionManager, gateNodeId string) gatetrans.ITransportor {
	trans := &transportorRpc{
		sessionMgr:   sessionMgr,
		logicNodeMgr: &logicNodeMgr{},
		gateNodeId:   gateNodeId,
	}

	rpcRouter := kkrpc.NewRpcReceiver()
	rpcProcessor := &rpcHandler{
		trans: trans,
	}
	kkrpc.RegistOneWayHandler(rpcRouter, "register", rpcProcessor.onRegister)
	kkrpc.RegistOneWayHandler(rpcRouter, "s2c", rpcProcessor.onS2C)
	kkrpc.RegistOneWayHandler(rpcRouter, "s2cs", rpcProcessor.onS2Clients)
	kkrpc.RegistOneWayHandler(rpcRouter, "c2s", rpcProcessor.onC2S)

	rpcSvr := kkrpc.NewServer("127.0.0.1:19090", kknet.DefaultOptions(), rpcRouter)
	if err := rpcSvr.Start(); err != nil {
		kklog.Errorf("[ccgate] start rpc server error: %v", err)
		return nil
	}

	trans.rpcSvr = rpcSvr
	return trans
}

func (slf *transportorRpc) ForwardToLogic(sessionID string, msgBytes []byte, logicNodeId string) error {
	memberInfo := slf.logicNodeMgr.getLogicNode(logicNodeId)
	if memberInfo == nil {
		return ErrLogicNodeNotRegistered //逻辑节点未注册
	}
	_, err := slf.sessionMgr.GetConn(sessionID)
	if err != nil {
		return err //客户端已下线
	}
	streamBytes := msgBytes
	oneWayInvoker := kkrpc.NewOneWayInvoker[ptotrans.RpcC2S](slf.rpcSvr, memberInfo.connId, "c2s")
	err = oneWayInvoker.InvokeNR(context.Background(), &ptotrans.RpcC2S{
		ClientId:   sessionID,
		GateNodeId: slf.gateNodeId,
		Payload:    streamBytes,
	}, kkrpc.CallConfig{})
	return nil
}

func (slf *transportorRpc) ForwardToClient(sessionID string, msgBytes []byte) error {
	if sessionID == "" {
		return kkerrors.ErrEmptySessionID
	}
	if len(msgBytes) == 0 {
		return kkerrors.ErrEmptyMsgBytes
	}
	conn, err := slf.sessionMgr.GetConn(sessionID)
	if err != nil {
		return err //客户端已下线
	}

	streamBytes := kkbuffer.GetWithLenCap(len(msgBytes), len(msgBytes))
	copy(streamBytes.B, msgBytes)
	if err := conn.SendBuffer(streamBytes); err != nil {
		kklog.Errorf("[ccgate] send response error: %v", err)
	}
	return nil
}

func (slf *transportorRpc) registerLogicNode(nodeId string, nodeType string, connId kknet.CONN_ID) {
	slf.logicNodeMgr.registerLogicNode(nodeId, nodeType, connId)
}

func (slf *transportorRpc) unregisterLogicNode(nodeId string) {
	slf.logicNodeMgr.unregisterLogicNode(nodeId)
}
