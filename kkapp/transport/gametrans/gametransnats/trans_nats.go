package gametransnats

import (
	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/transport/gametrans"
	"github.com/vvisun/kkdg/kkapp/transport/ptotrans"
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
	stopped     bool
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

func (slf *transportorNats) Stop() error {
	if slf.stopped {
		return nil
	}
	slf.stopped = true
	slf.cluster.Stop()
	return nil
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
	if slf.stopped {
		return kkerrors.ErrAppTransportorStopped
	}
	if sessionID == "" {
		return kkerrors.ErrAppEmptySessionID
	}
	if len(packet) == 0 {
		return kkerrors.ErrAppEmptyMsgBytes
	}

	sessionInfo := slf.sessionMgr.GetSession(sessionID)
	if sessionInfo == nil {
		return kkerrors.ErrAppSessionNotFound
	}

	resp := kkcluster.NewClusterPacket()
	resp.FuncName = ptotrans.FuncNameSendToClient
	resp.ArgBytes = packet //transportor编码时是复制，所以这里可以直接传引用，不用再复制一次。
	resp.Sid = sessionID
	if err := slf.cluster.PublishRemote(sessionInfo.GetGateNodeID(), resp); err != nil {
		kklog.Errorf("[ccgame] publish response to %s error: %v", sessionInfo.GetGateNodeID(), err)
		return err
	}
	return nil
}

// @param packet is a full stream packet [length,message]
func (slf *transportorNats) ForwardToClients(sessionIDs []string, packet []byte) error {
	if slf.stopped {
		return kkerrors.ErrAppTransportorStopped
	}
	if len(sessionIDs) == 0 {
		return nil
	}
	if len(packet) == 0 {
		return kkerrors.ErrAppEmptyMsgBytes
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
		sidByGateNodeID[sessionInfo.GetGateNodeID()] += sid + ","
	}

	for gateNodeID, sids := range sidByGateNodeID {
		resp := kkcluster.NewClusterPacket()
		resp.FuncName = ptotrans.FuncNameSendToClients
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
	if slf.stopped {
		return kkerrors.ErrAppTransportorStopped
	}
	if sessionID == "" {
		return kkerrors.ErrAppEmptySessionID
	}
	if msg == nil {
		return kkerrors.ErrPktInvalidMessage
	}

	sessionInfo := slf.sessionMgr.GetSession(sessionID)
	if sessionInfo == nil {
		return kkerrors.ErrAppSessionNotFound
	}

	bb, err := kkpacket.EncodeStream(msg, kkapp.GetStreamTool(), kkapp.GetMsgPacket())
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
	if slf.stopped {
		return kkerrors.ErrAppTransportorStopped
	}
	if len(sessionIDs) == 0 {
		return nil
	}
	if msg == nil {
		return kkerrors.ErrPktInvalidMessage
	}

	if len(sessionIDs) == 1 {
		return slf.SendToClient(sessionIDs[0], msg)
	}

	bb, err := kkpacket.EncodeStream(msg, kkapp.GetStreamTool(), kkapp.GetMsgPacket())
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

func (slf *transportorNats) NotifyClientLoginLogout(sessionID string, userId int64, isLogin bool) error {
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
	var msg ptotrans.RpcClientLoginLogout
	msg.ClientId = sessionID
	msg.UserId = userId
	msg.IsLogin = isLogin
	msg.NodeType = ""
	msg.NodeId = ""
	msg.GateNodeId = sessionInfo.GetGateNodeID()
	bbTrans, err := kkpacket.EncodeStream(&msg, kkapp.GetStreamTool(), kkapp.GetTransMsgPacket())
	if err != nil {
		return err
	}

	streamBytes := bbTrans.B //transportor编码时是复制，所以这里可以直接传引用，不用再复制一次。

	pkt := kkcluster.NewClusterPacket()
	pkt.FuncName = ptotrans.FuncNameClientLoginLogout
	pkt.ArgBytes = streamBytes
	pkt.Sid = sessionID
	err = slf.cluster.PublishRemote(sessionInfo.GetGateNodeID(), pkt)
	kkbuffer.Put(bbTrans)
	if err != nil {
		return err
	}
	return nil
}
