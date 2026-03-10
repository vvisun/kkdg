package gametransshard

import (
	"sync"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/comps/ccgame/gametrans"
	"github.com/vvisun/kkdg/kkapp/comps/ptotrans"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/msgreceiver"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

type transportorShard struct {
	conns   [kkapp.BackendShardCnt]*gatewayClient // 每个shard一个客户端，用于连接网关
	muConns sync.RWMutex

	sessionMgr  *gametrans.SessionManager
	msgReceiver *msgreceiver.MsgReceiver[string]
	gatewayAddr string
	nodeId      string
	nodeType    string
}

func NewTransportorShard(sessionMgr *gametrans.SessionManager, msgReceiver *msgreceiver.MsgReceiver[string], gatewayAddr, nodeID, nodeType string) (gametrans.ITransportor, error) {
	trans := &transportorShard{
		sessionMgr:  sessionMgr,
		msgReceiver: msgReceiver,
		gatewayAddr: gatewayAddr,
		nodeId:      nodeID,
		nodeType:    nodeType,
	}

	for i := 0; i < kkapp.BackendShardCnt; i++ {
		trans.conns[i] = NewGatewayClient(i, trans)
	}

	return trans, nil
}

func (slf *transportorShard) getConn(shardIdx int) *gatewayClient {
	if shardIdx < 0 {
		shardIdx = 0
	}
	shardIdx = shardIdx % kkapp.BackendShardCnt
	slf.muConns.RLock()
	conn := slf.conns[shardIdx]
	slf.muConns.RUnlock()
	return conn
}

// @param packet is a full stream packet [length,message]
func (slf *transportorShard) ForwardToClient(sessionID string, packet []byte) error {
	if sessionID == "" {
		return nil
	}
	if len(packet) == 0 {
		return kkerrors.ErrEmptyMsgBytes
	}
	sessionInfo := slf.sessionMgr.GetSession(sessionID)
	if sessionInfo == nil {
		return kkerrors.ErrSessionNotFound
	}
	conn := slf.getConn(sessionInfo.ShardIdx)
	if conn == nil {
		return kkerrors.ErrConnNotFound
	}
	payload := packet //EncodeStream会进行复制，这里可以直接传引用
	rpcMsg := &ptotrans.RpcS2Client{ClientId: sessionID, Payload: payload}
	bb, err := kkpacket.EncodeStream(rpcMsg, kkpacket.DefaultStreamPacket(), kkapp.GetTransMsgPacket())
	if err != nil {
		return err
	}
	return conn.cli.SendBuffer(bb)
}

// @param packet is a full stream packet [length,message]
func (slf *transportorShard) ForwardToClients(sessionIDs []string, packet []byte) error {
	if len(sessionIDs) == 0 {
		return nil //空sessionID列表返回正常
	}
	if len(packet) == 0 {
		return kkerrors.ErrEmptyMsgBytes
	}
	for _, sessionID := range sessionIDs {
		if sessionID == "" {
			continue
		}
		slf.ForwardToClient(sessionID, packet)
	}
	return nil
}

func (slf *transportorShard) SendToClient(sessionID string, msg any) error {
	if sessionID == "" {
		return nil //空sessionID返回正常
	}
	if msg == nil {
		return kkerrors.ErrInvalidMessage
	}
	sessionInfo := slf.sessionMgr.GetSession(sessionID)
	if sessionInfo == nil {
		return kkerrors.ErrSessionNotFound
	}
	conn := slf.getConn(sessionInfo.ShardIdx)
	if conn == nil {
		return kkerrors.ErrConnNotFound
	}
	bb, err := kkpacket.EncodeStream(msg, kkpacket.DefaultStreamPacket(), kkapp.GetMsgPacket())
	if err != nil {
		kkbuffer.Put(bb)
		return err
	}
	// 下行必须走转发协议 RpcS2Client，网关按 msgID=2 解析后 ForwardToClient(Payload) 再写 WS
	payload := bb.B //EncodeStream编码时是复制，所以这里可以直接传引用，不用再复制一次。
	rpcMsg := &ptotrans.RpcS2Client{ClientId: sessionID, Payload: payload}
	bbTrans, err := kkpacket.EncodeStream(rpcMsg, kkpacket.DefaultStreamPacket(), kkapp.GetTransMsgPacket())
	kkbuffer.Put(bb)
	if err != nil {
		return err
	}
	return conn.cli.SendBuffer(bbTrans)
}

func (slf *transportorShard) SendToClients(sessionIDs []string, msg any) error {
	if len(sessionIDs) == 0 {
		return nil //空之间返回正常
	}
	if msg == nil {
		return kkerrors.ErrInvalidMessage
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
