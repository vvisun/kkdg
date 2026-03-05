package ccgame

import (
	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/msgreceiver"
	"github.com/vvisun/kkdg/remotes/kkcluster"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

// ITransportor 数据转发器接口。
// 抽象化接口，方便切换实现逻辑（如：使用Actor、使用Nats、使用RPC等）。
type ITransportor interface {
	forwardToClient(sessionID string, messageBytes []byte) error
	onRecvMsg(sessionID string, streamBytes []byte) error
	SendToClient(sessionID string, msg any) error
}

//------------------------------------------------------------

// transportorNats 使用Nats集群转发消息
type transportorNats struct {
	cluster     kkcluster.ICluster // cluster for forwarding messages to client
	sessionMgr  *sessionManager
	msgReceiver *msgreceiver.MsgReceiver[string]
}

var _ ITransportor = (*transportorNats)(nil)

func newTransportorNats(cluster kkcluster.ICluster, msgReceiver *msgreceiver.MsgReceiver[string]) ITransportor {
	trans := &transportorNats{
		cluster:     cluster,
		sessionMgr:  newSessionManager(),
		msgReceiver: msgReceiver,
	}
	cluster.SetPublishHandler(trans.onPublish)
	return trans
}

// onPublish 收到来自其他节点的消息
func (slf *transportorNats) onPublish(sourceNodeID string, packet *kkcluster.ClusterPacket) {
	if packet == nil || packet.Sid == "" || len(packet.ArgBytes) == 0 {
		return
	}

	if slf.sessionMgr.GetSession(packet.Sid) == nil {
		slf.sessionMgr.AddSession(packet.Sid, sourceNodeID)
	}

	slf.onRecvMsg(packet.Sid, packet.ArgBytes)
}

func (slf *transportorNats) onRecvMsg(sessionID string, streamBytes []byte) error {
	if sessionID == "" {
		return kkerrors.ErrEmptySessionID
	}
	if len(streamBytes) == 0 {
		return kkerrors.ErrEmptyMsgBytes
	}

	// 处理来自客户端的消息
	slf.msgReceiver.OnSession(sessionID, streamBytes)

	return nil
}

// forwardToClient 转发消息到客户端
func (slf *transportorNats) forwardToClient(sessionID string, messageBytes []byte) error {
	if sessionID == "" {
		return kkerrors.ErrEmptySessionID
	}
	if len(messageBytes) == 0 {
		return kkerrors.ErrEmptyMsgBytes
	}
	sessionInfo := slf.sessionMgr.GetSession(sessionID)
	if sessionInfo == nil {
		return kkerrors.ErrSessionNotFound
	}

	resp := kkcluster.NewClusterPacket()
	resp.FuncName = "s2c" //暂时没用到
	resp.ArgBytes = append([]byte(nil), messageBytes...)
	resp.Sid = sessionID
	if err := slf.cluster.PublishRemote(sessionInfo.GateNodeID, resp); err != nil {
		kklog.Errorf("[ccgame] publish response to %s error: %v", sessionInfo.GateNodeID, err)
		return err
	}
	return nil
}

func (slf *transportorNats) SendToClient(sessionID string, msg any) error {
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
	err = slf.forwardToClient(sessionID, bb.B)
	kkbuffer.Put(bb)
	if err != nil {
		return err
	}
	return nil
}
