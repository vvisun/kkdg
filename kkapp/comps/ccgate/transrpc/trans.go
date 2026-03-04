package transrpc

import (
	"sync"

	"github.com/vvisun/kkdg/kkapp/comps/ccgate/transface"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/remotes/kkrpc"
	"github.com/vvisun/kkdg/utils/kklog"
)

type logicMemberInfo struct {
	nodeId   string
	nodeType string
	conn     kknet.IConn
}

// transportorRpc 使用RPC转发消息
// 逻辑服先连接到本网关, 然后发送[register:nodeId,nodeType]注册到本网关, 进行注册服务。
type transportorRpc struct {
	rpcSvr       *kkrpc.Server
	sessionMgr   transface.ISessionManager
	logicNodeMap sync.Map // map[string]*logicMemberInfo
}

var _ transface.ITransportor = (*transportorRpc)(nil)

func NewTransportorRpc() transface.ITransportor {
	rpcRouter := kkrpc.NewRpcReceiver()
	rpcProcessor := &rpcHandler{}
	kkrpc.RegistOneWayHandler(rpcRouter, "register", rpcProcessor.onRegister)
	kkrpc.RegistOneWayHandler(rpcRouter, "s2c", rpcProcessor.onS2C)

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
	memberInfo := slf.getLogicNode(logicNodeId)
	if memberInfo == nil {
		return ErrLogicNodeNotRegistered //逻辑节点未注册
	}
	_, err := slf.sessionMgr.GetConn(sessionID)
	if err != nil {
		return err //客户端已下线
	}
	bb, err := kkpacket.DefaultStreamPacket().Pack(msgBytes)
	if err != nil {
		return err //打包失败
	}
	if err := memberInfo.conn.SendBuffer(bb); err != nil {
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

	// packet.ArgBytes is [message], pack it to [length,message] then send back to client.
	bb, err := kkpacket.DefaultStreamPacket().Pack(msgBytes)
	if err != nil {
		kklog.Errorf("[ccgate] pack response error: %v", err)
		return err //打包失败
	}
	if err := conn.SendBuffer(bb); err != nil {
		kklog.Errorf("[ccgate] send response error: %v", err)
	}
	return nil
}

func (slf *transportorRpc) GetSessionMgr() transface.ISessionManager {
	return slf.sessionMgr
}

func (slf *transportorRpc) registerLogicNode(nodeId string, nodeType string, conn kknet.IConn) {
	memberInfo := &logicMemberInfo{
		nodeId:   nodeId,
		nodeType: nodeType,
		conn:     conn,
	}
	slf.logicNodeMap.Store(nodeId, memberInfo)
}

func (slf *transportorRpc) unregisterLogicNode(nodeId string) {
	slf.logicNodeMap.Delete(nodeId)
}

func (slf *transportorRpc) getLogicNode(nodeId string) *logicMemberInfo {
	value, ok := slf.logicNodeMap.Load(nodeId)
	if !ok {
		return nil
	}
	return value.(*logicMemberInfo)
}
