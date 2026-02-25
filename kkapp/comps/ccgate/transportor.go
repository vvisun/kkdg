package ccgate

import (
	"strconv"
	"sync"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/remotes/kkcluster"
	"github.com/vvisun/kkdg/utils/kklog"
)

//go:inline
func getSessionId(connID kknet.CONN_ID) string {
	return strconv.FormatUint(connID, 10)
}

type ISessionManager interface {
	// GetConn gets a client connection by sessionID
	GetConn(sessionID string) (kknet.IConn, error)
	// AddConn adds a client connection by sessionID
	AddConn(sessionID string, conn kknet.IConn)
	// RemoveConn removes a client connection by sessionID
	RemoveConn(sessionID string)
}

// ITransportor 数据转发器接口。
// 抽象化接口，方便切换实现逻辑（如：使用Actor、使用Nats、使用RPC等）。
type ITransportor interface {
	// ForwardToLogic forwards a client message to logic side.
	ForwardToLogic(sessionID string, msgRoute string, msgBytes []byte) error
	// ForwardToClient forwards a logic message to client side.
	ForwardToClient(sessionID string, msgBytes []byte) error
	// GetSessionMgr gets the session manager
	GetSessionMgr() ISessionManager
}

//------------------------------------------------------------

type sessionManager struct {
	connMap sync.Map // sessionID(string) -> kknet.IConn (client connection)
}

var _ ISessionManager = (*sessionManager)(nil)

func newSessionMgr() ISessionManager {
	return &sessionManager{
		connMap: sync.Map{},
	}
}

func (slf *sessionManager) GetConn(sessionID string) (kknet.IConn, error) {
	if sessionID == "" {
		return nil, ErrEmptySessionID
	}
	v, ok := slf.connMap.Load(sessionID)
	if !ok {
		return nil, ErrSessionNotFound
	}
	return v.(kknet.IConn), nil
}

func (slf *sessionManager) AddConn(sessionID string, conn kknet.IConn) {
	if sessionID == "" {
		return
	}
	if conn == nil {
		return
	}
	slf.connMap.Store(sessionID, conn)
}

func (slf *sessionManager) RemoveConn(sessionID string) {
	if sessionID == "" {
		return
	}
	slf.connMap.Delete(sessionID)
}

//------------------------------------------------------------

// transportorNats 使用Nats集群转发消息
type transportorNats struct {
	cluster    kkcluster.ICluster // cluster for forwarding messages to logic and client
	sessionMgr ISessionManager
}

var _ ITransportor = (*transportorNats)(nil)

func NewTransportorNats(cluster kkcluster.ICluster) ITransportor {
	trans := &transportorNats{
		cluster:    cluster,
		sessionMgr: newSessionMgr(),
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
func (slf *transportorNats) ForwardToLogic(sessionID string, msgRoute string, msgBytes []byte) error {
	if slf.cluster == nil {
		return ErrClusterNotInitialized
	}
	if sessionID == "" {
		return ErrEmptySessionID
	}
	if len(msgBytes) == 0 {
		return nil
	}

	pkt := kkcluster.NewClusterPacket()
	pkt.FuncName = msgRoute
	pkt.ArgBytes = append([]byte(nil), msgBytes...)
	pkt.Sid = sessionID
	return slf.cluster.PublishRemoteType(kkapp.NodeTypeLogic, pkt)
}

// ForwardToClient 转发消息到客户端
func (slf *transportorNats) ForwardToClient(sessionID string, msgBytes []byte) error {
	if sessionID == "" {
		return ErrEmptySessionID
	}
	conn, err := slf.sessionMgr.GetConn(sessionID)
	if err != nil {
		return err
	}
	if len(msgBytes) == 0 {
		return ErrEmptyMsgBytes
	}

	// packet.ArgBytes is [message], pack it to [length,message] then send back to client.
	bb, err := kkpacket.DefaultStreamPacket().Pack(msgBytes)
	if err != nil {
		kklog.Errorf("[ccgate] pack response error: %v", err)
		return err
	}
	if err := conn.SendBuffer(bb); err != nil {
		kklog.Errorf("[ccgate] send response error: %v", err)
	}
	return nil
}

func (slf *transportorNats) GetSessionMgr() ISessionManager {
	return slf.sessionMgr
}
