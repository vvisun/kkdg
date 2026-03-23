package gametransshard

import (
	"sync"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/transport"
	"github.com/vvisun/kkdg/kkapp/transport/gametrans"
	"github.com/vvisun/kkdg/kkapp/transport/ptotrans"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

type transportorShard struct {
	conns   [transport.BackendShardCnt]*gatewayClient // 每个shard一个客户端，用于连接网关
	muConns sync.RWMutex

	sessionMgr       *gametrans.SessionManager
	msgReceiver      gametrans.ISessionMsgReceiver
	gatewayAddr      string
	nodeId           string
	nodeType         string
	stopped          bool
	transMsgPacket   *kkpacket.MessagePacket
	clientMsgPacket  *kkpacket.MessagePacket
	transStreamTool  kkpacket.IPacket
	clientStreamTool kkpacket.IPacket
}

func NewTransportorShard(
	sessionMgr *gametrans.SessionManager,
	msgReceiver gametrans.ISessionMsgReceiver,
	gatewayAddr string,
	nodeInfo kkapp.INodeIdentity,
	transMsgPacket *kkpacket.MessagePacket,
	clientMsgPacket *kkpacket.MessagePacket,
	transStreamTool kkpacket.IPacket,
	clientStreamTool kkpacket.IPacket,
) (gametrans.ITransportor, error) {
	ptotrans.InitShardMsgs(transMsgPacket.GetRouter())
	trans := &transportorShard{
		sessionMgr:       sessionMgr,
		msgReceiver:      msgReceiver,
		gatewayAddr:      gatewayAddr,
		nodeId:           nodeInfo.GetNodeId(),
		nodeType:         nodeInfo.GetNodeType(),
		transMsgPacket:   transMsgPacket,
		clientMsgPacket:  clientMsgPacket,
		transStreamTool:  transStreamTool,
		clientStreamTool: clientStreamTool,
	}

	for i := 0; i < transport.BackendShardCnt; i++ {
		trans.conns[i] = NewGatewayClient(i, trans)
	}

	return trans, nil
}

func (slf *transportorShard) Stop() error {
	if slf.stopped {
		return nil
	}
	slf.stopped = true
	slf.muConns.Lock()
	conns := make([]*gatewayClient, 0, transport.BackendShardCnt)
	copy(conns, slf.conns[:])
	slf.muConns.Unlock()

	kklog.Infof("[分流] 停止分流，发送 RpcUnregister 到网关")

	sended := false
	for _, conn := range conns {
		if conn != nil {
			if !sended {
				if err := slf.sendRpcUnregister(conn); err == nil {
					sended = true
				} else {
					kklog.Warnf("[分流 %d] 发送 RpcUnregister 失败: %v", conn.shardIdx, err)
				}
			}
			_ = conn.cli.Close()
		}
	}
	return nil
}

func (slf *transportorShard) sendRpcUnregister(conn *gatewayClient) error {
	msg := ptotrans.RpcUnregister{
		NodeId: slf.nodeId,
	}
	bb, err := kkpacket.EncodeStream(&msg, slf.transStreamTool, slf.transMsgPacket)
	if err != nil {
		kkbuffer.Put(bb)
		return err
	}
	return conn.cli.SendBuffer(bb)
}

func (slf *transportorShard) getConn(shardIdx int) *gatewayClient {
	if shardIdx < 0 {
		shardIdx = 0
	}
	shardIdx = shardIdx % transport.BackendShardCnt
	slf.muConns.RLock()
	conn := slf.conns[shardIdx]
	slf.muConns.RUnlock()
	return conn
}

// @param packet is a full stream packet [length,message]
func (slf *transportorShard) ForwardToClient(sessionID string, packet []byte) error {
	if slf.stopped {
		return kkerrors.ErrAppTransportorStopped
	}
	if sessionID == "" {
		return nil
	}
	if len(packet) == 0 {
		return kkerrors.ErrAppEmptyMsgBytes
	}
	sessionInfo := slf.sessionMgr.GetSession(sessionID)
	if sessionInfo == nil {
		return kkerrors.ErrAppSessionNotFound
	}
	conn := slf.getConn(sessionInfo.GetShardIdx())
	if conn == nil {
		return kkerrors.ErrNetConnNotFound
	}
	payload := packet //EncodeStream会进行复制，这里可以直接传引用
	rpcMsg := &ptotrans.RpcS2Client{ClientId: sessionID, Payload: payload}
	bb, err := kkpacket.EncodeStream(rpcMsg, slf.transStreamTool, slf.transMsgPacket)
	if err != nil {
		return err
	}
	return conn.cli.SendBuffer(bb)
}

// @param packet is a full stream packet [length,message]
func (slf *transportorShard) ForwardToClients(sessionIDs []string, packet []byte) error {
	if slf.stopped {
		return kkerrors.ErrAppTransportorStopped
	}
	if len(sessionIDs) == 0 {
		return nil //空sessionID列表返回正常
	}
	if len(packet) == 0 {
		return kkerrors.ErrAppEmptyMsgBytes
	}
	var loopErr error
	for _, sessionID := range sessionIDs {
		if sessionID == "" {
			continue
		}
		err := slf.ForwardToClient(sessionID, packet)
		if err != nil {
			loopErr = err
		}
	}
	return loopErr
}

func (slf *transportorShard) SendToClient(sessionID string, msg any) error {
	if slf.stopped {
		return kkerrors.ErrAppTransportorStopped
	}
	if sessionID == "" {
		return nil //空sessionID返回正常
	}
	if msg == nil {
		return kkerrors.ErrPktInvalidMessage
	}
	sessionInfo := slf.sessionMgr.GetSession(sessionID)
	if sessionInfo == nil {
		return kkerrors.ErrAppSessionNotFound
	}
	conn := slf.getConn(sessionInfo.GetShardIdx())
	if conn == nil {
		return kkerrors.ErrNetConnNotFound
	}
	bb, err := kkpacket.EncodeStream(msg, slf.clientStreamTool, slf.clientMsgPacket)
	if err != nil {
		kkbuffer.Put(bb)
		return err
	}
	// 下行必须走转发协议 RpcS2Client，网关按 msgID=2 解析后 ForwardToClient(Payload) 再写 WS
	payload := bb.B //EncodeStream编码时是复制，所以这里可以直接传引用，不用再复制一次。
	rpcMsg := &ptotrans.RpcS2Client{ClientId: sessionID, Payload: payload}
	bbTrans, err := kkpacket.EncodeStream(rpcMsg, slf.transStreamTool, slf.transMsgPacket)
	kkbuffer.Put(bb)
	if err != nil {
		return err
	}
	return conn.cli.SendBuffer(bbTrans)
}

func (slf *transportorShard) SendToClients(sessionIDs []string, msg any) error {
	if slf.stopped {
		return kkerrors.ErrAppTransportorStopped
	}
	if len(sessionIDs) == 0 {
		return nil //空之间返回正常
	}
	if msg == nil {
		return kkerrors.ErrPktInvalidMessage
	}
	var loopErr error
	for _, sessionID := range sessionIDs {
		if sessionID == "" {
			continue
		}
		err := slf.SendToClient(sessionID, msg)
		if err != nil {
			loopErr = err
		}
	}
	return loopErr
}

func (slf *transportorShard) NotifyClientLoginLogout(sessionID string, userId int64, isLogin bool) error {
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
	conn := slf.getConn(sessionInfo.GetShardIdx())
	if conn == nil {
		return kkerrors.ErrNetConnNotFound
	}
	var msg ptotrans.RpcClientLoginLogout
	msg.ClientId = sessionID
	msg.UserId = userId
	msg.IsLogin = isLogin
	msg.NodeType = slf.nodeType
	msg.NodeId = slf.nodeId
	msg.GateNodeId = sessionInfo.GetGateNodeID()
	bbTrans, err := kkpacket.EncodeStream(&msg, slf.transStreamTool, slf.transMsgPacket)
	if err != nil {
		return err
	}
	return conn.cli.SendBuffer(bbTrans)
}

func (slf *transportorShard) CloseClient(sessionID string, reason string) error {
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
	conn := slf.getConn(sessionInfo.GetShardIdx())
	if conn == nil {
		return kkerrors.ErrNetConnNotFound
	}
	var msg ptotrans.RpcCloseClient
	msg.ClientId = sessionID
	msg.GateNodeId = sessionInfo.GetGateNodeID()
	msg.Reason = reason
	bbTrans, err := kkpacket.EncodeStream(&msg, slf.transStreamTool, slf.transMsgPacket)
	if err != nil {
		return err
	}
	return conn.cli.SendBuffer(bbTrans)
}

func (slf *transportorShard) GetSessionManager() *gametrans.SessionManager {
	return slf.sessionMgr
}
