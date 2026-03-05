package transnat

import (
	"github.com/vvisun/kkdg/kkapp/comps/ccgate/transface"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/remotes/kkcluster"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

// transportorNats 使用Nats集群转发消息
type transportorNats struct {
	cluster    kkcluster.ICluster // cluster for forwarding messages to logic and client
	sessionMgr transface.ISessionManager
}

var _ transface.ITransportor = (*transportorNats)(nil)

func NewTransportorNats(cluster kkcluster.ICluster) transface.ITransportor {
	trans := &transportorNats{
		cluster:    cluster,
		sessionMgr: transface.NewSessionMgr(),
	}
	cluster.SetPublishHandler(trans.onPublish)
	return trans
}

// onPublish 收到来自其他节点的消息，转发给客户端
func (slf *transportorNats) onPublish(nodeID string, packet *kkcluster.ClusterPacket) {
	if packet == nil || packet.Sid == "" || len(packet.ArgBytes) == 0 {
		return
	}
	slf.ForwardToClient(packet.Sid, packet.ArgBytes)
}

// ForwardToLogic 转发消息到逻辑节点
func (slf *transportorNats) ForwardToLogic(sessionID string, msgBytes []byte, logicNodeId string) error {
	if slf.cluster == nil {
		return kkerrors.ErrClusterNotInitialized
	}
	if sessionID == "" {
		return kkerrors.ErrEmptySessionID
	}
	if len(msgBytes) == 0 {
		return nil
	}

	pkt := kkcluster.NewClusterPacket()
	pkt.FuncName = "c2s"    //暂时没用到
	pkt.ArgBytes = msgBytes //transportor编码时是复制，所以这里可以直接传引用，不用再复制一次。
	pkt.Sid = sessionID
	return slf.cluster.PublishRemote(logicNodeId, pkt)
}

// ForwardToClient 转发消息到客户端
func (slf *transportorNats) ForwardToClient(sessionID string, msgBytes []byte) error {
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

	if err := conn.SendBuffer(kkbuffer.NewByteBuffer(msgBytes)); err != nil {
		kklog.Errorf("[ccgate] send response error: %v", err)
	}
	return nil
}

func (slf *transportorNats) GetSessionMgr() transface.ISessionManager {
	return slf.sessionMgr
}
