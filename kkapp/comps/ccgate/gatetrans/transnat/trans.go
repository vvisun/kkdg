package transnat

import (
	"strings"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/comps/ccgate/gatetrans"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/remotes/kkcluster"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

// transportorNats 使用Nats集群转发消息
type transportorNats struct {
	cluster    kkcluster.ICluster // cluster for forwarding messages to logic and client
	sessionMgr gatetrans.ISessionManager
}

var _ gatetrans.ITransportor = (*transportorNats)(nil)

func NewTransportorNats(cluster kkcluster.ICluster, sessionMgr gatetrans.ISessionManager) gatetrans.ITransportor {
	trans := &transportorNats{
		cluster:    cluster,
		sessionMgr: sessionMgr,
	}
	cluster.SetPublishHandler(trans.onPublish)
	return trans
}

// onPublish 收到来自其他节点的消息，转发给客户端
func (slf *transportorNats) onPublish(nodeID string, packet *kkcluster.ClusterPacket) {
	if packet == nil || packet.Sid == "" || len(packet.ArgBytes) == 0 {
		return
	}
	switch packet.FuncName {
	case kkapp.FuncNameSendToClient:
		slf.ForwardToClient(packet.Sid, packet.ArgBytes)
	case kkapp.FuncNameSendToClients:
		sids := strings.Split(packet.Sid, ",")
		for _, sid := range sids {
			slf.ForwardToClient(sid, packet.ArgBytes)
		}
	}
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
	pkt.FuncName = kkapp.FuncNameC2S //暂时没用到
	pkt.ArgBytes = msgBytes          //transportor编码时是复制，所以这里可以直接传引用，不用再复制一次。
	pkt.Sid = sessionID
	return slf.cluster.PublishRemote(logicNodeId, pkt)
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

	conn, err := slf.sessionMgr.GetConn(sessionID)
	if err != nil {
		return err //客户端已下线
	}

	// 这里需要复制，因为传递过来的msgBytes可能会被其他地方回收修改。
	streamBytes := kkbuffer.GetWithCapacity(len(packet))
	streamBytes.WriteBytes(packet)

	if err := conn.SendBuffer(streamBytes); err != nil {
		kklog.Errorf("[ccgate] send response error: %v", err)
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

	for _, sessionID := range sessionIDs {
		if sessionID == "" {
			continue
		}

		conn, err := slf.sessionMgr.GetConn(sessionID)
		if err != nil {
			continue //客户端已下线
		}

		// 这里需要复制，因为传递过来的msgBytes可能会被其他地方回收修改。
		// 而且SendBuffer会自动释放streamBytes。所以需要复制一份。
		streamBytes := kkbuffer.GetWithCapacity(len(packet))
		streamBytes.WriteBytes(packet)

		if err := conn.SendBuffer(streamBytes); err != nil {
			kklog.Errorf("[ccgate] send response error: %v", err)
		}
	}
	return nil
}

func (slf *transportorNats) NotifyClientDisconnect(sessionID string, logicNodeId string, connId kknet.CONN_ID) error {
	if slf.cluster == nil {
		return kkerrors.ErrClusterNotInitialized
	}
	if sessionID == "" {
		return kkerrors.ErrEmptySessionID
	}

	pkt := kkcluster.NewClusterPacket()
	pkt.FuncName = kkapp.FuncNameClientDisconnect
	pkt.ArgBytes = nil
	pkt.Sid = sessionID
	return slf.cluster.PublishRemote(logicNodeId, pkt)
}
