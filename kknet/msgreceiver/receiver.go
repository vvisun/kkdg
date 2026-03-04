package msgreceiver

import (
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

type MetaParser func(data *kkbuffer.ByteBuffer) (kkpacket.MSGID, []byte, error)

// MsgReceiver 消息接收器
type MsgReceiver[K any] struct {
	messagePacket *kkpacket.MessagePacket
	hdMap         map[kkpacket.MSGID]IMsgHandler[K] // 消息ID到消息处理器的映射
	metaParser    MetaParser
}

var _ kknet.IRawHandler = (*MsgReceiver[kknet.CONN_ID])(nil) // 默认实现

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

// OnRaw 接收原始数据并分发到消息处理器
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

func (r *MsgReceiver[K]) OnSession(sessionID K, messageBytes []byte) {
	msgID, err := r.messagePacket.GetMsgID(messageBytes)
	if err != nil {
		return
	}
	bodyBytes, err := r.messagePacket.BodyBytes(messageBytes)
	if err != nil {
		return
	}

	h, ok := r.hdMap[msgID]
	if !ok || h == nil {
		return
	}

	h.OnRaw(sessionID, bodyBytes)
}

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

// RegisterMsgHandler 注册消息处理器
// 非线程安全，一般在初始化时调用，故不考虑线程安全
func RegisterMsgHandler[T any, K any](receiver *MsgReceiver[K], call MsgHandlerFunc[T, K]) {
	var v T
	msgID := receiver.messagePacket.GetRouter().GetMsgID(&v)
	if msgID == 0 {
		kklog.Error("message type not registered")
		return
	}
	h := NewMsgHandler[T, K](msgID, receiver.messagePacket.GetBodyCodec(), call)
	receiver.hdMap[msgID] = h
}
