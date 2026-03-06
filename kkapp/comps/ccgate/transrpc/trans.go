package transrpc

import (
	"sync"

	"github.com/vvisun/kkdg/kkapp/comps/ccgate/transface"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/remotes/kkrpc"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

type logicMemberInfo struct {
	nodeId   string
	nodeType string
	conn     kknet.IConn
}

type logicNodeMgr struct {
	logicNodeMap sync.Map // map[string]*logicMemberInfo
}

func (slf *logicNodeMgr) registerLogicNode(nodeId string, nodeType string, conn kknet.IConn) {
	memberInfo := &logicMemberInfo{
		nodeId:   nodeId,
		nodeType: nodeType,
		conn:     conn,
	}
	slf.logicNodeMap.Store(nodeId, memberInfo)
}

func (slf *logicNodeMgr) unregisterLogicNode(nodeId string) {
	slf.logicNodeMap.Delete(nodeId)
}

func (slf *logicNodeMgr) getLogicNode(nodeId string) *logicMemberInfo {
	value, ok := slf.logicNodeMap.Load(nodeId)
	if !ok {
		return nil
	}
	return value.(*logicMemberInfo)
}

// transportorRpc 使用RPC转发消息
// 逻辑服先连接到本网关, 然后发送[register:nodeId,nodeType]注册到本网关, 进行注册服务。
type transportorRpc struct {
	rpcSvr       *kkrpc.Server
	sessionMgr   transface.ISessionManager
	logicNodeMgr *logicNodeMgr
}

var _ transface.ITransportor = (*transportorRpc)(nil)

func NewTransportorRpc() transface.ITransportor {
	rpcRouter := kkrpc.NewRpcReceiver()
	rpcProcessor := &rpcHandler{}
	kkrpc.RegistOneWayHandler(rpcRouter, "register", rpcProcessor.onRegister)
	kkrpc.RegistOneWayHandler(rpcRouter, "s2c", rpcProcessor.onS2C)
	kkrpc.RegistOneWayHandler(rpcRouter, "s2cs", rpcProcessor.onS2Clients)
	kkrpc.RegistOneWayHandler(rpcRouter, "c2s", rpcProcessor.onC2S)

	rpcSvr := kkrpc.NewServer("", kknet.DefaultOptions(), rpcRouter)
	if err := rpcSvr.Start(); err != nil {
		kklog.Errorf("[ccgate] start rpc server error: %v", err)
		return nil
	}

	return &transportorRpc{
		rpcSvr:     rpcSvr,
		sessionMgr: transface.NewSessionMgr(),
	}
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
	streamBytes := kkbuffer.GetWithCapacity(len(msgBytes))
	copy(streamBytes.B, msgBytes)
	if err := memberInfo.conn.SendBuffer(streamBytes); err != nil {
		return err //发送失败
	}
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

	streamBytes := kkbuffer.GetWithCapacity(len(msgBytes))
	copy(streamBytes.B, msgBytes)
	if err := conn.SendBuffer(streamBytes); err != nil {
		kklog.Errorf("[ccgate] send response error: %v", err)
	}
	return nil
}

func (slf *transportorRpc) GetSessionMgr() transface.ISessionManager {
	return slf.sessionMgr
}

func (slf *transportorRpc) registerLogicNode(nodeId string, nodeType string, conn kknet.IConn) {
	slf.logicNodeMgr.registerLogicNode(nodeId, nodeType, conn)
}

func (slf *transportorRpc) unregisterLogicNode(nodeId string) {
	slf.logicNodeMgr.unregisterLogicNode(nodeId)
}
