package gametransshard

import (
	"net"
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
	gatewayAddr string
	nodeId      string
	nodeType    string
	msgReceiver *msgreceiver.MsgReceiver[string]
}

func NewTransportorShard(sessionMgr *gametrans.SessionManager, gatewayAddr string, nodeID, nodeType string) gametrans.ITransportor {
	trans := &transportorShard{
		sessionMgr:  sessionMgr,
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

func (slf *transportorShard) ForwardToClient(sessionID string, messageBytes []byte) error {
	if sessionID == "" || len(messageBytes) == 0 {
		return nil
	}
	sessionInfo := slf.sessionMgr.GetSession(sessionID)
	if sessionInfo == nil {
		return kkerrors.ErrSessionNotFound
	}
	shardIdx := sessionInfo.ShardIdx % kkapp.BackendShardCnt
	if shardIdx < 0 {
		shardIdx = 0
	}
	conn := slf.conns[shardIdx]
	if conn == nil {
		return kkerrors.ErrConnNotFound
	}
	streamBytes := messageBytes
	_, _ = conn.Write(streamBytes)
	return nil
}

func (slf *transportorShard) ForwardToClients(sessionIDs []string, messageBytes []byte) error {
	if len(sessionIDs) == 0 || len(messageBytes) == 0 {
		return nil
	}
	for _, sessionID := range sessionIDs {
		if sessionID == "" {
			continue
		}
		slf.ForwardToClient(sessionID, messageBytes)
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
	shardIdx := sessionInfo.ShardIdx % kkapp.BackendShardCnt
	conn := slf.conns[shardIdx]
	if conn == nil {
		return kkerrors.ErrConnNotFound
	}
	bb, err := kkpacket.EncodeStream(msg, kkpacket.DefaultStreamPacket(), kkapp.GetMsgPacket())
	if err != nil {
		kkbuffer.Put(bb)
		return err
	}
	streamBytes := bb.B
	_, _ = conn.Write(streamBytes)
	kkbuffer.Put(bb)
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
				bb, err := kkpacket.EncodeStream(msg, kkpacket.DefaultStreamPacket(), kkapp.GetTransMsgPacket())
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
		businessLoop(idx, connectGateway(idx, trans), trans)
	}()

	for {
		data := []byte{} //这里应该读网络数据

		messageBytes, err := kkpacket.DefaultStreamPacket().MessageBytes(data)
		if err != nil {
			return
		}
		msgID, err := kkapp.GetTransMsgPacket().GetMsgID(messageBytes)
		if err != nil {
			return
		}
		bodyBytes, err := kkapp.GetTransMsgPacket().BodyBytes(messageBytes)
		if err != nil {
			return
		}

		switch msgID {
		case 4: // 网关转发客户端消息到逻辑服: 客户端->网关->逻辑服
			var msg ptotrans.RpcC2S
			err = kkapp.GetTransMsgPacket().GetBodyCodec().Unmarshal(bodyBytes, &msg)
			if err != nil {
				return
			}
			if trans.sessionMgr.GetSession(msg.ClientId) == nil {
				trans.sessionMgr.AddSession(msg.ClientId, msg.GateNodeId)
			}
			streamBytes := msg.Payload
			trans.msgReceiver.OnSession(msg.ClientId, streamBytes)
		case 5: // 客户端断开事件
			var msg ptotrans.RpcClientDisconnect
			err = kkapp.GetTransMsgPacket().GetBodyCodec().Unmarshal(bodyBytes, &msg)
			if err != nil {
				return
			}
			// 在这里写：离线清理、存库、踢下线、房间退出等逻辑
			kklog.Debugf("[逻辑服%d] 玩家断开 clientId=%s clientIds=%v", idx, msg.ClientId, msg.ClientIds)
			trans.sessionMgr.RemoveSession(msg.ClientId)
			for _, clientId := range msg.ClientIds {
				trans.sessionMgr.RemoveSession(clientId)
			}
		}
	}
}
