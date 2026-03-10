package gametransnats

import (
	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/comps/ccgame/gametrans"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/msgreceiver"
	"github.com/vvisun/kkdg/remotes/kkcluster"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

// transportorNats 使用Nats集群转发消息
type transportorNats struct {
	cluster     kkcluster.ICluster // cluster for forwarding messages to client
	sessionMgr  *gametrans.SessionManager
	msgReceiver *msgreceiver.MsgReceiver[string]
}

func NewTransportorNats(cluster kkcluster.ICluster, msgReceiver *msgreceiver.MsgReceiver[string], sessionManager *gametrans.SessionManager) (gametrans.ITransportor, error) {
	trans := &transportorNats{
		cluster:     cluster,
		sessionMgr:  sessionManager,
		msgReceiver: msgReceiver,
	}
	msgReceiver.SetNeedCopyInOnSession(false)
	cluster.SetPublishHandler(trans.onPublish)
	return trans, nil
}

// onPublish 收到来自其他节点的消息
func (slf *transportorNats) onPublish(sourceNodeID string, packet *kkcluster.ClusterPacket) {
	if packet == nil || packet.Sid == "" || len(packet.ArgBytes) == 0 {
		return
	}

	if slf.sessionMgr.GetSession(packet.Sid) == nil {
		slf.sessionMgr.AddSession(packet.Sid, sourceNodeID)
	}

	// 处理来自客户端的消息
	slf.msgReceiver.OnSession(packet.Sid, packet.ArgBytes)
}

// ForwardToClient 转发消息到客户端
// @param packet is a full stream packet [length,message]
func (slf *transportorNats) ForwardToClient(sessionID string, packet []byte) error {
	if sessionID == "" {
		return kkerrors.ErrEmptySessionID
	}
	if len(packet) == 0 {
		return kkerrors.ErrEmptyMsgBytes
	}

	sessionInfo := slf.sessionMgr.GetSession(sessionID)
	if sessionInfo == nil {
		return kkerrors.ErrSessionNotFound
	}

	resp := kkcluster.NewClusterPacket()
	resp.FuncName = kkapp.FuncNameSendToClient
	resp.ArgBytes = packet //transportor编码时是复制，所以这里可以直接传引用，不用再复制一次。
	resp.Sid = sessionID
	if err := slf.cluster.PublishRemote(sessionInfo.GateNodeID, resp); err != nil {
		kklog.Errorf("[ccgame] publish response to %s error: %v", sessionInfo.GateNodeID, err)
		return err
	}
	return nil
}

// @param packet is a full stream packet [length,message]
func (slf *transportorNats) ForwardToClients(sessionIDs []string, packet []byte) error {
	if len(sessionIDs) == 0 {
		return nil
	}
	if len(packet) == 0 {
		return kkerrors.ErrEmptyMsgBytes
	}

	if len(sessionIDs) == 1 {
		return slf.ForwardToClient(sessionIDs[0], packet)
	}

	sidByGateNodeID := make(map[string]string)
	for _, sid := range sessionIDs {
		sessionInfo := slf.sessionMgr.GetSession(sid)
		if sessionInfo == nil {
			continue
		}
		sidByGateNodeID[sessionInfo.GateNodeID] += sid + ","
	}

	for gateNodeID, sids := range sidByGateNodeID {
		resp := kkcluster.NewClusterPacket()
		resp.FuncName = kkapp.FuncNameSendToClients
		resp.ArgBytes = packet //transportor编码时是复制，所以这里可以直接传引用，不用再复制一次。
		resp.Sid = sids
		if err := slf.cluster.PublishRemote(gateNodeID, resp); err != nil {
			kklog.Errorf("[ccgame] publish response to %s error: %v", gateNodeID, err)
			return err
		}
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
	streamBytes := bb.B //transportor编码时是复制，所以这里可以直接传引用，不用再复制一次。
	err = slf.ForwardToClient(sessionID, streamBytes)
	kkbuffer.Put(bb)
	if err != nil {
		return err
	}
	return nil
}

func (slf *transportorNats) SendToClients(sessionIDs []string, msg any) error {
	if len(sessionIDs) == 0 {
		return nil
	}
	if msg == nil {
		return kkerrors.ErrInvalidMessage
	}

	if len(sessionIDs) == 1 {
		return slf.SendToClient(sessionIDs[0], msg)
	}

	bb, err := kkpacket.EncodeStream(msg, kkpacket.DefaultStreamPacket(), kkapp.GetMsgPacket())
	if err != nil {
		kkbuffer.Put(bb)
		return err
	}
	streamBytes := bb.B //transportor编码时是复制，所以这里可以直接传引用，不用再复制一次。
	err = slf.ForwardToClients(sessionIDs, streamBytes)
	kkbuffer.Put(bb)
	if err != nil {
		return err
	}
	return nil
}
