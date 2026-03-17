package msgreceiver

import (
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/byteslice"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/queues/taskqueue"
)

const workers_count = 4096

/**自定义解析完整包数据[length,message]。
 *@param data []byte 完整包数据[length,message]
 *@return kkpacket.MSGID 消息ID
 *@return []byte 消息对象二进制数据
 *@return error 错误
 */
type MetaParser func(data []byte) (kkpacket.MSGID, []byte, error)

// MsgReceiver 消息接收器
type MsgReceiver[K any] struct {
	packetTool    *kkpacket.FullPacket
	hdMap         map[kkpacket.MSGID]IMsgHandler[K] // 消息ID到消息处理器的映射
	metaParser    MetaParser
	decodeWorkers []*taskqueue.WorkerQueue
}

/** 解析完整包数据[length,message]。
 *@param packet []byte 完整包数据[length,message]
 *@return kkpacket.MSGID 消息ID
 *@return []byte 消息对象二进制数据
 *@return error 错误
 */
func (r *MsgReceiver[K]) parseMsgInfo(packet []byte) (kkpacket.MSGID, []byte, error) {
	if r.metaParser != nil {
		return r.metaParser(packet)
	}

	// 默认实现
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

// OnRaw 接收原始数据并分发到消息处理器。实现kknet.IRawHandler接口。
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

// OnSession 接收来自会话的消息并分发到消息处理器。
//
//	@param sessionID 会话ID
//	@param packet 整包数据[length,message]。不得保存 packet 引用，如需保存，请自行拷贝。
func (r *MsgReceiver[K]) OnSession(sessionID K, packet []byte, shardIdx int) {
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

	r.decodeWorkers[shardIdx].Push(func() {
		h.OnMessage(sessionID, bodyCopy)
	})
}

//--------------------------------------------------

// NewMsgReceiver 创建消息接收器
func NewMsgReceiver[K any](packetTool *kkpacket.FullPacket) *MsgReceiver[K] {
	workers := make([]*taskqueue.WorkerQueue, workers_count)
	for i := 0; i < workers_count; i++ {
		workers[i] = taskqueue.NewWorkerQueue(1)
	}
	return &MsgReceiver[K]{
		packetTool:    packetTool,
		hdMap:         make(map[kkpacket.MSGID]IMsgHandler[K]),
		metaParser:    nil,
		decodeWorkers: workers,
	}
}

// NewMsgReceiverWithParser 创建消息接收器，使用自定义的元数据解析器
func NewMsgReceiverWithParser[K any](packetTool *kkpacket.FullPacket, metaParser MetaParser) *MsgReceiver[K] {
	workers := make([]*taskqueue.WorkerQueue, workers_count)
	for i := 0; i < workers_count; i++ {
		workers[i] = taskqueue.NewWorkerQueue(1)
	}
	return &MsgReceiver[K]{
		packetTool:    packetTool,
		hdMap:         make(map[kkpacket.MSGID]IMsgHandler[K]),
		metaParser:    metaParser,
		decodeWorkers: workers,
	}
}
