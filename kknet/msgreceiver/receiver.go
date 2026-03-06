package msgreceiver

import (
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

type MetaParser func(data *kkbuffer.ByteBuffer) (kkpacket.MSGID, []byte, error)

// MsgReceiver 消息接收器
type MsgReceiver[K any] struct {
	messagePacket       *kkpacket.MessagePacket
	hdMap               map[kkpacket.MSGID]IMsgHandler[K] // 消息ID到消息处理器的映射
	metaParser          MetaParser
	needCopyInOnSession bool
}

// 设置是否需要在内部分配新的buffer来处理session消息
// @note if use transportor rpc. need copy streamBytes to a new buffer.
// @note if use transportor nats. not need copy.
func (r *MsgReceiver[K]) SetNeedCopyInOnSession(isNeedCopy bool) {
	r.needCopyInOnSession = isNeedCopy
}

// 解析出: msgID-消息ID，bodyBytes-消息对象二进制数据
func (r *MsgReceiver[K]) parseMsgInfo(data *kkbuffer.ByteBuffer) (kkpacket.MSGID, []byte, error) {
	if r.metaParser != nil {
		return r.metaParser(data)
	}
	// 默认实现
	messageBytes, err := kkpacket.DefaultStreamPacket().Unpack(data.Bytes())
	if err != nil {
		return 0, nil, err
	}
	msgId, err := r.messagePacket.GetMsgID(messageBytes)
	if err != nil {
		return 0, nil, err
	}
	bodyBytes, err := r.messagePacket.BodyBytes(messageBytes)
	if err != nil {
		return 0, nil, err
	}
	return msgId, bodyBytes, nil
}

var _ kknet.IRawHandler = (*MsgReceiver[kknet.CONN_ID])(nil)

// OnRaw 接收原始数据并分发到消息处理器。实现kknet.IRawHandler接口。
// @param connId 连接ID
// @param data 原始数据 完整包[length,message]
func (r *MsgReceiver[K]) OnRaw(connId K, data *kkbuffer.ByteBuffer) {
	msgID, bodyBytes, err := r.parseMsgInfo(data)
	if err != nil {
		kkbuffer.Put(data)
		return
	}

	h, ok := r.hdMap[msgID]
	if !ok || h == nil {
		kkbuffer.Put(data)
		return
	}

	h.OnRaw(connId, bodyBytes)
	kkbuffer.Put(data)
}

// OnSession 接收来自会话的消息并分发到消息处理器。
// @param sessionID 会话ID
// @param messageBytes 消息数据 packet的[message]部分
func (r *MsgReceiver[K]) OnSession(sessionID K, streamBytes []byte) {
	// if use transportor rpc. need copy streamBytes to a new buffer.
	// if use transportor nats. not need copy.
	var data *kkbuffer.ByteBuffer
	if r.needCopyInOnSession {
		data = kkbuffer.GetWithCapacity(len(streamBytes))
		data.WriteBytes(streamBytes)
	} else {
		data = kkbuffer.NewByteBuffer(streamBytes)
	}

	msgID, bodyBytes, err := r.parseMsgInfo(data)
	if err != nil {
		kkbuffer.Put(data)
		return
	}

	h, ok := r.hdMap[msgID]
	if !ok || h == nil {
		kkbuffer.Put(data)
		return
	}

	h.OnRaw(sessionID, bodyBytes)
	kkbuffer.Put(data)
}

//--------------------------------------------------

// NewMsgReceiver 创建消息接收器
func NewMsgReceiver[K any](messagePacket *kkpacket.MessagePacket) *MsgReceiver[K] {
	return &MsgReceiver[K]{
		messagePacket: messagePacket,
		hdMap:         make(map[kkpacket.MSGID]IMsgHandler[K]),
		metaParser:    nil,
	}
}

// NewMsgReceiverWithParser 创建消息接收器，使用自定义的元数据解析器
func NewMsgReceiverWithParser[K any](messagePacket *kkpacket.MessagePacket, metaParser MetaParser) *MsgReceiver[K] {
	return &MsgReceiver[K]{
		messagePacket: messagePacket,
		hdMap:         make(map[kkpacket.MSGID]IMsgHandler[K]),
		metaParser:    metaParser,
	}
}
