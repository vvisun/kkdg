package gametransshard

import (
	"net"
	"sync"
	"time"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/comps/ccgame/gametrans"
	"github.com/vvisun/kkdg/kkapp/comps/ptotrans"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/msgreceiver"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/xnet"
)

type transportorShard struct {
	sessionMgr  *gametrans.SessionManager
	conns       [kkapp.BackendShardCnt]net.Conn
	muConns     sync.RWMutex
	gatewayAddr string
	nodeId      string
	nodeType    string
	msgReceiver *msgreceiver.MsgReceiver[string]
}

func NewTransportorShard(sessionMgr *gametrans.SessionManager, msgReceiver *msgreceiver.MsgReceiver[string], gatewayAddr, nodeID, nodeType string) gametrans.ITransportor {
	trans := &transportorShard{
		sessionMgr:  sessionMgr,
		msgReceiver: msgReceiver,
		gatewayAddr: gatewayAddr,
		nodeId:      nodeID,
		nodeType:    nodeType,
	}

	for i := 0; i < kkapp.BackendShardCnt; i++ {
		trans.conns[i] = connectGateway(i, trans)
	}

	for i := 0; i < kkapp.BackendShardCnt; i++ {
		go businessLoop(i, trans.conns[i], trans)
	}

	return trans
}

func (slf *transportorShard) getConn(shardIdx int) net.Conn {
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
	if sessionID == "" || len(packet) == 0 {
		return nil
	}
	sessionInfo := slf.sessionMgr.GetSession(sessionID)
	if sessionInfo == nil {
		return kkerrors.ErrSessionNotFound
	}
	conn := slf.getConn(sessionInfo.ShardIdx)
	if conn == nil {
		return kkerrors.ErrConnNotFound
	}
	// 下行必须走转发协议 RpcS2Client，网关按 msgID=2 解析后 ForwardToClient(Payload)
	payload := packet //EncodeStream会进行复制，这里可以直接传引用
	rpcMsg := &ptotrans.RpcS2Client{ClientId: sessionID, Payload: payload}
	bb, err := kkpacket.EncodeStream(rpcMsg, kkpacket.DefaultStreamPacket(), kkapp.GetTransMsgPacket())
	if err != nil {
		return err
	}
	_, _ = conn.Write(bb.B)
	kkbuffer.Put(bb)
	return nil
}

// @param packet is a full stream packet [length,message]
func (slf *transportorShard) ForwardToClients(sessionIDs []string, packet []byte) error {
	if len(sessionIDs) == 0 || len(packet) == 0 {
		return nil
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
	if sessionID == "" || msg == nil {
		return nil
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
	_, _ = conn.Write(bbTrans.B)
	kkbuffer.Put(bbTrans)
	return nil
}

func (slf *transportorShard) SendToClients(sessionIDs []string, msg any) error {
	if len(sessionIDs) == 0 || msg == nil {
		return nil
	}
	for _, sessionID := range sessionIDs {
		if sessionID == "" {
			continue
		}
		slf.SendToClient(sessionID, msg)
	}
	return nil
}

// 重连网关
func connectGateway(idx int, trans *transportorShard) net.Conn {
	for {
		conn, err := net.Dial("tcp", trans.gatewayAddr)
		if err == nil {
			kklog.Infof("分流[%d] 连接网关成功", idx)
			xnet.SetNoDelay(conn, true)

			go func() {
				time.Sleep(100 * time.Millisecond)
				// 将自己注册到网关
				msg := ptotrans.RpcMsgRegister{
					ShardIdx: idx,
					NodeId:   trans.nodeId,
					NodeType: trans.nodeType,
				}
				bb, err := kkpacket.EncodeStream(&msg, kkpacket.DefaultStreamPacket(), kkapp.GetTransMsgPacket())
				if err == nil {
					_, _ = conn.Write(bb.B)
				}
				kkbuffer.Put(bb)
			}()

			return conn
		}
		kklog.Infof("分流[%d] 连接失败，重试中", idx)
		time.Sleep(1 * time.Second)
	}
}

// 业务处理 + 断开事件清理
func businessLoop(idx int, conn net.Conn, trans *transportorShard) {
	defer func() {
		_ = conn.Close()
		time.Sleep(1 * time.Second)
		newConn := connectGateway(idx, trans)
		trans.muConns.Lock()
		trans.conns[idx] = newConn
		trans.muConns.Unlock()
		businessLoop(idx, newConn, trans)
	}()

	stream := kkpacket.DefaultStreamPacket()
	lfb := stream.LengthFieldByteCount()
	recvBuf := make([]byte, 0, 16*1024)
	tmp := make([]byte, 4*1024)
	recvs := make([][]byte, 0, 8)

	for {
		n, err := conn.Read(tmp)
		if err != nil {
			return
		}
		if n == 0 {
			continue
		}
		recvBuf = append(recvBuf, tmp[:n]...)
		packets, left, err := stream.Split(recvBuf, recvs)
		if err != nil {
			kklog.Warnf("[逻辑服%d] 拆包错误: %v", idx, err)
			return
		}
		recvBuf = recvBuf[:0]
		if len(left) > 0 {
			recvBuf = append(recvBuf, left...)
		}

		for _, pkt := range packets {
			if len(pkt) < lfb {
				continue
			}
			messageBytes, err := stream.MessageBytes(pkt)
			if err != nil {
				continue
			}
			msgID, err := kkapp.GetTransMsgPacket().GetMsgID(messageBytes)
			if err != nil {
				kklog.Warnf("[逻辑服%d] GetMsgID: %v", idx, err)
				continue
			}
			bodyBytes, err := kkapp.GetTransMsgPacket().BodyBytes(messageBytes)
			if err != nil {
				continue
			}

			switch msgID {
			case 4: // 网关转发客户端消息到逻辑服: 客户端->网关->逻辑服
				var msg ptotrans.RpcC2S
				err = kkapp.GetTransMsgPacket().GetBodyCodec().Unmarshal(bodyBytes, &msg)
				if err != nil {
					kklog.Warnf("[逻辑服%d] 解析 RpcC2S: %v", idx, err)
					continue
				}
				if trans.sessionMgr.GetSession(msg.ClientId) == nil {
					trans.sessionMgr.AddSessionWithShard(msg.ClientId, msg.GateNodeId, idx)
				}
				trans.msgReceiver.OnSession(msg.ClientId, msg.Payload)
			case 5: // 客户端断开事件
				var msg ptotrans.RpcClientDisconnect
				err = kkapp.GetTransMsgPacket().GetBodyCodec().Unmarshal(bodyBytes, &msg)
				if err != nil {
					kklog.Warnf("[逻辑服%d] 解析 RpcClientDisconnect: %v", idx, err)
					continue
				}
				kklog.Debugf("[逻辑服%d] 玩家断开 clientId=%s clientIds=%v", idx, msg.ClientId, msg.ClientIds)
				trans.sessionMgr.RemoveSession(msg.ClientId)
				for _, clientId := range msg.ClientIds {
					trans.sessionMgr.RemoveSession(clientId)
				}
			}
		}
	}
}
