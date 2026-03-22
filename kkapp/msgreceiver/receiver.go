package msgreceiver

import (
	"github.com/vvisun/kkdg/kkapp/transport/gametrans"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/byteslice"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/queues/taskqueue"
)

// NewMsgReceiver 创建消息接收器
func NewMsgReceiver[K any](packetTool *kkpacket.FullPacket) *MsgReceiver[K] {
	return &MsgReceiver[K]{
		packetTool: packetTool,
		hdMap:      make(map[kkpacket.MSGID]IMsgHandler[K]),
	}
}

// NewSessionMsgReceiver 创建会话消息接收器
func NewSessionMsgReceiver[K any](packetTool *kkpacket.FullPacket, workersCount int) *MsgReceiver[K] {
	decodeWorkers := make([]*taskqueue.WorkerQueue, workersCount)
	for i := 0; i < workersCount; i++ {
		decodeWorkers[i] = taskqueue.NewWorkerQueue(1)
	}
	return &MsgReceiver[K]{
		packetTool:    packetTool,
		hdMap:         make(map[kkpacket.MSGID]IMsgHandler[K]),
		decodeWorkers: decodeWorkers,
	}
}

// MsgReceiver 消息接收器
type MsgReceiver[K any] struct {
	packetTool    *kkpacket.FullPacket
	hdMap         map[kkpacket.MSGID]IMsgHandler[K] // 消息ID到消息处理器的映射
	decodeWorkers []*taskqueue.WorkerQueue          // 解码工作队列, 并行解码消息。用于游戏服的会话消息接收器。
}

/** 解析完整包数据[length,message]。
 *@param packet []byte 完整包数据[length,message]
 *@return kkpacket.MSGID 消息ID
 *@return []byte 消息对象二进制数据
 *@return error 错误
 */
func (r *MsgReceiver[K]) parseMsgInfo(packet []byte) (kkpacket.MSGID, []byte, error) {
	messageBytes, err := r.packetTool.GetStreamTool().Unpack(packet)
	if err != nil {
		return 0, nil, err
	}
	msgId, err := r.packetTool.GetMessageTool().GetMsgID(messageBytes)
	if err != nil {
		return 0, nil, err
	}
	bodyBytes, err := r.packetTool.GetMessageTool().BodyBytes(messageBytes)
	if err != nil {
		return 0, nil, err
	}
	return msgId, bodyBytes, nil
}

var _ kknet.IRawHandler = (*MsgReceiver[kknet.CONN_ID])(nil)
var _ gametrans.ISessionMsgReceiver = (*MsgReceiver[string])(nil)

// OnRaw 实现kknet.IRawHandler接口。接收原始数据并分发到消息处理器。
//
//	用法1：直接将MsgReceiver当做IRawHandler作为kknet.Options的RawHandler选项。
//	用法2：自己创建IRawHandler实现，将MsgReceiver作为自定义IRawHandler的成员，在OnRaw方法中调用msgReceiver.OnRaw。
//
//	@param connId kknet.CONN_ID
//	@param bbPacket 原始数据 完整包[length,message]
func (r *MsgReceiver[K]) OnRaw(connId K, bbPacket *kkbuffer.ByteBuffer) {
	msgID, bodyBytes, err := r.parseMsgInfo(bbPacket.Bytes())
	if err != nil {
		kkbuffer.Put(bbPacket)
		return
	}

	h, ok := r.hdMap[msgID]
	if !ok || h == nil {
		kkbuffer.Put(bbPacket)
		return
	}

	h.OnMessage(connId, bodyBytes)
	kkbuffer.Put(bbPacket)
}

// OnSession 实现gametrans.ISessionMsgReceiver接口。接收来自会话的消息并分发到消息处理器。
//
//	@param sessionID 会话ID
//	@param packet 整包数据[length,message]。不得保存 packet 引用，如需保存，请自行拷贝。
func (r *MsgReceiver[K]) OnSession(sessionID K, packet []byte, threadIdx int) {
	msgID, bodyBytes, err := r.parseMsgInfo(packet)
	if err != nil {
		return
	}

	h, ok := r.hdMap[msgID]
	if !ok || h == nil {
		return
	}

	bodyCopy := byteslice.GetWithLenCap(len(bodyBytes), len(bodyBytes))
	copy(bodyCopy, bodyBytes)

	r.decodeWorkers[threadIdx].Push(func() {
		h.OnMessage(sessionID, bodyCopy)
	})
}
