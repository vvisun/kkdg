package transshard

import (
	"sync"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/comps/ccgate/gatetrans"
	"github.com/vvisun/kkdg/kkapp/comps/ptotrans"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kkprocessor"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

type transportorShard struct {
	logicServerMgr *LogicServerMgr
	logicConnMgr   sync.Map // map[connId]*ShardConn
	server         kknet.IServer
	sessionMgr     gatetrans.ISessionManager
	gateNodeId     string
}

var _ gatetrans.ITransportor = (*transportorShard)(nil)

func NewTransportorShard(addr string, sessionMgr gatetrans.ISessionManager, nodeId string) gatetrans.ITransportor {
	handler := &shardHandler{}
	serOpts := kknet.ApplyOptions(
		kknet.WithRawHandler(handler),
		kknet.WithRpProvider(kkprocessor.NewReadProcessor),
		kknet.WithWpProvider(kkprocessor.NewWriteProcessor),
		kknet.WithRecvQueueSize(1024),
		kknet.WithWorkerQueueMaxConcurrency(1),
		kknet.WithBufferSizes(4*1024, 4*1024),
	)
	srv := kktcp.NewServer(addr, handler, serOpts)
	if err := srv.Start(); err != nil {
		kklog.Fatalf("start shard server error: %v", err)
	}

	trans := &transportorShard{
		logicServerMgr: &LogicServerMgr{},
		server:         srv,
		sessionMgr:     sessionMgr,
		gateNodeId:     nodeId,
	}
	handler.transporter = trans
	return trans
}

func (slf *transportorShard) ForwardToLogic(sessionID string, msgBytes []byte, logicNodeId string) error {
	chooseServer := slf.logicServerMgr.getLogicServer(logicNodeId)
	if chooseServer == nil {
		return kkerrors.ErrLogicNodeNotRegistered //逻辑节点未注册
	}
	cConn, err := slf.sessionMgr.GetConn(sessionID)
	if err != nil {
		return err //客户端已下线
	}

	var msg ptotrans.RpcC2S
	msg.ClientId = sessionID
	msg.GateNodeId = slf.gateNodeId
	msg.Payload = msgBytes
	bb, err := kkpacket.EncodeStream(&msg, kkpacket.DefaultStreamPacket(), kkapp.GetTransMsgPacket())
	if err != nil {
		kkbuffer.Put(bb)
		return err
	}

	shardIdx := cConn.ID() % kkapp.BackendShardCnt
	chooseServer.muConns.RLock()
	sconn := chooseServer.conns[shardIdx]
	chooseServer.muConns.RUnlock()
	if sconn == nil {
		kkbuffer.Put(bb)
		return nil
	}

	if err := sconn.conn.SendBuffer(bb); err != nil {
		return err
	}

	return nil
}

func (slf *transportorShard) ForwardToClient(sessionID string, msgBytes []byte) error {
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

	// 这里需要复制，因为传递过来的msgBytes可能会被其他地方回收修改。
	streamBytes := kkbuffer.GetWithCapacity(len(msgBytes))
	streamBytes.WriteBytes(msgBytes)

	if err := conn.SendBuffer(streamBytes); err != nil {
		kklog.Errorf("[ccgate] send response error: %v", err)
	}
	return nil
}

func (slf *transportorShard) ForwardToClients(sessionIDs []string, msgBytes []byte) error {
	if len(sessionIDs) == 0 {
		return nil
	}
	if len(msgBytes) == 0 {
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
		streamBytes := kkbuffer.GetWithCapacity(len(msgBytes))
		streamBytes.WriteBytes(msgBytes)

		if err := conn.SendBuffer(streamBytes); err != nil {
			kklog.Errorf("[ccgate] send response error: %v", err)
		}
	}
	return nil
}

type shardHandler struct {
	transporter *transportorShard
}

func (h *shardHandler) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	defer kkbuffer.Put(data)
	messageBytes, err := kkpacket.DefaultStreamPacket().MessageBytes(data.B)
	if err != nil {
		kklog.Errorf("shard handler on raw get message bytes error: %v", err)
		return
	}
	msgID, err := kkapp.GetTransMsgPacket().GetMsgID(messageBytes)
	if err != nil {
		kklog.Errorf("shard handler on raw get message id error: %v", err)
		return
	}
	bodyBytes, err := kkapp.GetTransMsgPacket().BodyBytes(messageBytes)
	if err != nil {
		kklog.Errorf("shard handler on raw get body bytes error: %v", err)
		return
	}
	switch msgID {
	case 1: // 注册逻辑服
		var msg ptotrans.RpcMsgRegister
		kkapp.GetTransMsgPacket().GetBodyCodec().Unmarshal(bodyBytes, &msg)
		h.transporter.logicServerMgr.addLogicServer(&msg)
		if sc, ok := h.transporter.logicConnMgr.Load(connID); ok {
			h.transporter.logicServerMgr.addShardConn(msg.NodeId, msg.ShardIdx, sc.(*ShardConn))
		}
	case 2: // 网关转发消息到客户端: 逻辑服->网关->客户端
		var msg ptotrans.RpcS2Client
		kkapp.GetTransMsgPacket().GetBodyCodec().Unmarshal(bodyBytes, &msg)
		h.transporter.ForwardToClient(msg.ClientId, msg.Payload)
	case 3: // 网关转发消息到多个客户端: 逻辑服->网关->多个客户端
		var msg ptotrans.RpcS2Clients
		kkapp.GetTransMsgPacket().GetBodyCodec().Unmarshal(bodyBytes, &msg)
		h.transporter.ForwardToClients(msg.ClientIds, msg.Payload)
	default:
		kklog.Errorf("shard handler on raw unknown message id: %d", msgID)
	}
}

func (h *shardHandler) OnNoneCopy(connID kknet.CONN_ID, data []byte) {

}

func (h *shardHandler) OnConnect(conn kknet.IConn) {
	shardConn := &ShardConn{
		conn:     conn,
		connId:   conn.ID(),
		shardIdx: -1,
		nodeId:   "",
	}
	h.transporter.logicConnMgr.LoadOrStore(shardConn.connId, shardConn)
}

func (h *shardHandler) OnClose(conn kknet.IConn, err error) {
	sc, ok := h.transporter.logicConnMgr.Load(conn.ID())
	if !ok {
		return
	}
	h.transporter.logicConnMgr.Delete(conn.ID())
	shardConn := sc.(*ShardConn)
	h.transporter.logicServerMgr.removeShardConn(shardConn.nodeId, shardConn.shardIdx)
	kklog.Infof("shard conn closed: connId=%d nodeId=%s shardIdx=%d err=%v",
		conn.ID(), shardConn.nodeId, shardConn.shardIdx, err)
	shardConn.clear()
}
