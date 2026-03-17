package transshard

import (
	"sync"

	"github.com/vvisun/kkdg/kkapp/transport"
	"github.com/vvisun/kkdg/kkapp/transport/gatetrans"
	"github.com/vvisun/kkdg/kkapp/transport/ptotrans"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kkprocessor"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

type transportorShard struct {
	logicServerMgr   *LogicServerMgr
	shardConnMap     sync.Map // map[connId]*ShardConn, cache all ShardConn instances.
	server           kknet.IServer
	sessionMgr       gatetrans.ISessionManager
	gateNodeId       string
	stopped          bool
	msgHooker        *gatetrans.MsgHooker
	transMsgPacket   *kkpacket.MessagePacket
	clientMsgPacket  *kkpacket.MessagePacket
	transStreamTool  kkpacket.IPacket
	clientStreamTool kkpacket.IPacket
}

var _ gatetrans.ITransportor = (*transportorShard)(nil)
var _ gatetrans.IMemberMgrGetter = (*transportorShard)(nil)

func NewTransportorShard(
	addr string,
	sessionMgr gatetrans.ISessionManager,
	nodeId string,
	transMsgPacket *kkpacket.MessagePacket,
	clientMsgPacket *kkpacket.MessagePacket,
	transStreamTool kkpacket.IPacket,
	clientStreamTool kkpacket.IPacket,
) (gatetrans.ITransportor, error) {
	ptotrans.InitShardMsgs(transMsgPacket.GetRouter())
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
		kklog.Errorf("start shard server error: %v", err)
		return nil, err
	}

	trans := &transportorShard{
		logicServerMgr:   newLogicServerMgr(),
		server:           srv,
		sessionMgr:       sessionMgr,
		gateNodeId:       nodeId,
		msgHooker:        gatetrans.NewMsgHooker(),
		transMsgPacket:   transMsgPacket,
		clientMsgPacket:  clientMsgPacket,
		transStreamTool:  transStreamTool,
		clientStreamTool: clientStreamTool,
	}
	handler.transporter = trans
	return trans, nil
}

func (slf *transportorShard) Stop() error {
	if slf.stopped {
		return nil
	}
	slf.stopped = true
	return slf.server.Stop()
}

// 为了保证单个连接的消息顺序性，需要将连接分配到固定的分片索引。
func (slf *transportorShard) getShardIdx(connId kknet.CONN_ID) int {
	return int(connId % transport.BackendShardCnt)
}

// sendToLogicShard 向指定逻辑服的指定 shard 发送已编码包。
func (slf *transportorShard) sendToLogicShard(logicNodeId string, shardIdx int, bb *kkbuffer.ByteBuffer) error {
	sconn, err := slf.logicServerMgr.getShardConn(logicNodeId, shardIdx%transport.BackendShardCnt)
	if err != nil {
		kkbuffer.Put(bb)
		return err
	}
	return sconn.conn.SendBuffer(bb)
}

func (slf *transportorShard) ForwardToLogic(sessionID string, msgBytes []byte, logicNodeId string) error {
	if slf.stopped {
		return kkerrors.ErrAppTransportorStopped
	}
	if len(msgBytes) == 0 {
		return kkerrors.ErrAppEmptyMsgBytes
	}
	cConn, err := slf.sessionMgr.GetConn(sessionID)
	if err != nil {
		return err
	}
	var msg ptotrans.RpcC2S
	msg.ClientId = sessionID
	msg.GateNodeId = slf.gateNodeId
	msg.Payload = msgBytes
	bb, err := kkpacket.EncodeStream(&msg, slf.transStreamTool, slf.transMsgPacket)
	if err != nil {
		kkbuffer.Put(bb)
		return err
	}
	shardIdx := slf.getShardIdx(cConn.ID())
	return slf.sendToLogicShard(logicNodeId, shardIdx, bb)
}

// @param packet is a full stream packet [length,message]
func (slf *transportorShard) ForwardToClient(sessionID string, packet []byte) error {
	if slf.stopped {
		return kkerrors.ErrAppTransportorStopped
	}
	if sessionID == "" {
		return nil
	}
	if len(packet) == 0 {
		return kkerrors.ErrAppEmptyMsgBytes
	}

	conn, err := slf.sessionMgr.GetConn(sessionID)
	if err != nil {
		return err //客户端已下线
	}

	// 这里需要复制，因为传递过来的msgBytes可能会被其他地方回收修改。
	streamBytes := kkbuffer.GetWithCapacity(len(packet))
	streamBytes.WriteBytes(packet)

	if err := conn.SendBuffer(streamBytes); err != nil {
		kklog.Debugf("[ccgate] send response error: %v", err)
		return err
	}
	return nil
}

// @param packet is a full stream packet [length,message]
func (slf *transportorShard) ForwardToClients(sessionIDs []string, packet []byte) error {
	if slf.stopped {
		return kkerrors.ErrAppTransportorStopped
	}
	if len(sessionIDs) == 0 {
		return nil
	}
	if len(packet) == 0 {
		return kkerrors.ErrAppEmptyMsgBytes
	}

	var loopErr error
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
			kklog.Debugf("[ccgate] send response error: %v", err)
			loopErr = err
		}
	}
	return loopErr
}

func (slf *transportorShard) NotifyClientConnect(sessionID string, logicNodeId string, connId kknet.CONN_ID) error {
	if slf.stopped {
		return kkerrors.ErrAppTransportorStopped
	}
	var msg ptotrans.RpcAllocClient
	msg.ClientId = sessionID
	bb, err := kkpacket.EncodeStream(&msg, slf.transStreamTool, slf.transMsgPacket)
	if err != nil {
		kkbuffer.Put(bb)
		return err
	}
	shardIdx := slf.getShardIdx(connId)
	return slf.sendToLogicShard(logicNodeId, shardIdx, bb)
}

func (slf *transportorShard) NotifyClientDisconnect(sessionID string, logicNodeId string, connId kknet.CONN_ID) error {
	if slf.stopped {
		return kkerrors.ErrAppTransportorStopped
	}
	var msg ptotrans.RpcClientDisconnect
	msg.ClientId = sessionID
	bb, err := kkpacket.EncodeStream(&msg, slf.transStreamTool, slf.transMsgPacket)
	if err != nil {
		kkbuffer.Put(bb)
		return err
	}
	shardIdx := slf.getShardIdx(connId)
	return slf.sendToLogicShard(logicNodeId, shardIdx, bb)
}

func (slf *transportorShard) GetMemberMgr() gatetrans.IMemberMgr {
	return slf.logicServerMgr
}

func (slf *transportorShard) HookMsg(listener gatetrans.MsgHookListener) {
	slf.msgHooker.AddListener(listener)
}

//------------------------------------------------------------

type shardHandler struct {
	transporter *transportorShard
}

func (h *shardHandler) OnConnect(conn kknet.IConn) {
	// 先记录到shardConnMap中，正式成为逻辑分片还需要逻辑服主动发送 RpcMsgRegister 消息进行注册。
	shardConn := &ShardConn{
		conn:     conn,
		connId:   conn.ID(),
		shardIdx: -1,
		nodeId:   "",
	}
	h.transporter.shardConnMap.LoadOrStore(shardConn.connId, shardConn)
}

func (h *shardHandler) OnClose(conn kknet.IConn, err error) {
	sc, ok := h.transporter.shardConnMap.Load(conn.ID())
	if !ok {
		return
	}
	connId := conn.ID()
	shardConn := sc.(*ShardConn)
	kklog.Infof("逻辑服shard连接关闭: nodeId=%s shardIdx=%d connId=%d err=%v",
		shardConn.nodeId,
		shardConn.shardIdx,
		connId,
		err,
	)
	// 移除分片
	h.transporter.logicServerMgr.removeShardConn(shardConn.nodeId, shardConn.shardIdx)
	// 移除shardConnMap中的记录
	h.transporter.shardConnMap.Delete(connId)
	// 清空shardConn
	shardConn.clear()
}

func (h *shardHandler) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	defer kkbuffer.Put(data)
	transStreamTool := h.transporter.transStreamTool
	messageBytes, err := transStreamTool.MessageBytes(data.B)
	if err != nil {
		kklog.Errorf("shard handler on raw get message bytes error: %v", err)
		return
	}
	transMsgPacket := h.transporter.transMsgPacket
	msgID, err := transMsgPacket.GetMsgID(messageBytes)
	if err != nil {
		kklog.Errorf("shard handler on raw get message id error: %v", err)
		return
	}
	bodyBytes, err := transMsgPacket.BodyBytes(messageBytes)
	if err != nil {
		kklog.Errorf("shard handler on raw get body bytes error: %v", err)
		return
	}
	switch msgID {
	case ptotrans.MsgIDRpcMsgRegister: // 注册逻辑服
		var msg ptotrans.RpcMsgRegister
		transMsgPacket.GetBodyCodec().Unmarshal(bodyBytes, &msg)
		h.transporter.logicServerMgr.addLogicServer(&msg)
		if sc, ok := h.transporter.shardConnMap.Load(connID); ok {
			h.transporter.logicServerMgr.addShardConn(msg.NodeId, msg.ShardIdx, sc.(*ShardConn))
		}
	case ptotrans.MsgIDRpcS2Client: // 网关转发消息到客户端: 逻辑服->网关->客户端
		var msg ptotrans.RpcS2Client
		transMsgPacket.GetBodyCodec().Unmarshal(bodyBytes, &msg)
		h.transporter.ForwardToClient(msg.ClientId, msg.Payload)
	case ptotrans.MsgIDRpcS2Clients: // 网关转发消息到多个客户端: 逻辑服->网关->多个客户端
		var msg ptotrans.RpcS2Clients
		transMsgPacket.GetBodyCodec().Unmarshal(bodyBytes, &msg)
		h.transporter.ForwardToClients(msg.ClientIds, msg.Payload)
	case ptotrans.MsgIDRpcClientLoginLogout: // 逻辑服 -> 网关：客户端登入登出事件
		var msg ptotrans.RpcClientLoginLogout
		transMsgPacket.GetBodyCodec().Unmarshal(bodyBytes, &msg)
		h.transporter.msgHooker.Notify(msgID, &msg)
	case ptotrans.MsgIDRpcUnregister: // 逻辑服 -> 网关：逻辑服注销事件
		var msg ptotrans.RpcUnregister
		transMsgPacket.GetBodyCodec().Unmarshal(bodyBytes, &msg)
		h.transporter.logicServerMgr.removeLogicServer(msg.NodeId)
	default:
		kklog.Errorf("shard handler on raw unknown message id: %d", msgID)
	}
}

func (h *shardHandler) OnNoneCopy(connID kknet.CONN_ID, data []byte) {

}
