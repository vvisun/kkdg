package ccgame

import (
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/remotes/kkcluster"
	"github.com/vvisun/kkdg/utils/kklog"
)

// ITransportor 数据转发器接口。
// 抽象化接口，方便切换实现逻辑（如：使用Actor、使用Nats、使用RPC等）。
type ITransportor interface {
	ForwardToClient(sessionID string, msgBytes []byte) error
	OnRecvMsg(sessionID string, msgBytes []byte) error
}

//------------------------------------------------------------

// transportorNats 使用Nats集群转发消息
type transportorNats struct {
	cluster    kkcluster.ICluster // cluster for forwarding messages to client
	sessionMgr *sessionManager
}

var _ ITransportor = (*transportorNats)(nil)

func newTransportorNats(cluster kkcluster.ICluster) ITransportor {
	trans := &transportorNats{
		cluster:    cluster,
		sessionMgr: newSessionManager(),
	}
	cluster.SetPublishHandler(trans.onPublish)
	return trans
}

// onPublish 收到来自其他节点的消息
func (slf *transportorNats) onPublish(sourceNodeID string, packet *kkcluster.ClusterPacket) {
	if packet == nil || packet.Sid == "" || len(packet.ArgBytes) == 0 {
		return
	}

	if _, ok := slf.sessionMgr.GetSession(packet.Sid); !ok {
		slf.sessionMgr.AddSession(packet.Sid, sourceNodeID)
	}

	slf.OnRecvMsg(packet.Sid, packet.ArgBytes)
}

func (slf *transportorNats) OnRecvMsg(sessionID string, msgBytes []byte) error {
	if sessionID == "" {
		return kkerrors.ErrEmptySessionID
	}
	if len(msgBytes) == 0 {
		return kkerrors.ErrEmptyMsgBytes
	}
	// TODO: 处理来自客户端的消息。暂时直接回显
	slf.ForwardToClient(sessionID, msgBytes)
	return nil
}

// ForwardToClient 转发消息到客户端
func (slf *transportorNats) ForwardToClient(sessionID string, msgBytes []byte) error {
	if sessionID == "" {
		return kkerrors.ErrEmptySessionID
	}
	if len(msgBytes) == 0 {
		return kkerrors.ErrEmptyMsgBytes
	}
	sessionInfo, ok := slf.sessionMgr.GetSession(sessionID)
	if !ok {
		return kkerrors.ErrSessionNotFound
	}

	resp := kkcluster.NewClusterPacket()
	resp.FuncName = "s2c" //暂时没用到
	resp.ArgBytes = append([]byte(nil), msgBytes...)
	resp.Sid = sessionID
	if err := slf.cluster.PublishRemote(sessionInfo.GateNodeID, resp); err != nil {
		kklog.Errorf("[ccgame] publish response to %s error: %v", sessionInfo.GateNodeID, err)
		return err
	}
	return nil
}
