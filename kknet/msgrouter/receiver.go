package msgrouter

import (
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

// MsgReceiver 消息接收器
type MsgReceiver struct {
	messagePacket *kkpacket.MessagePacket
	m             map[interface{}]IMsgHandler // 消息ID到消息处理器的映射
}

// OnRaw 接收原始数据并分发到消息处理器
func (r *MsgReceiver) OnRaw(connId kknet.CONN_ID, data *kkbuffer.ByteBuffer) error {
	messageBytes, err := kkpacket.DefaultStreamPacket().Unpack(data.Bytes())
	if err != nil {
		kkbuffer.Put(data)
		return err
	}

	msgID, bodyBytes, err := kkpacket.ParseMsgInfo(messageBytes, r.messagePacket.GetHead())
	if err != nil {
		kkbuffer.Put(data)
		return err
	}

	h, ok := r.m[msgID]
	if !ok || h == nil {
		return kkerrors.ErrMsgHandlerNotRegistered
	}
	err = h.OnRaw(connId, bodyBytes)
	kkbuffer.Put(data)
	if err != nil {
		return err
	}
	return nil
}

// NewMsgReceiver 创建消息接收器
func NewMsgReceiver(messagePacket *kkpacket.MessagePacket) *MsgReceiver {
	return &MsgReceiver{
		messagePacket: messagePacket,
		m:             make(map[interface{}]IMsgHandler),
	}
}

// RegisterMsgHandler 注册消息处理器
func RegisterMsgHandler[T any](receiver *MsgReceiver, call MsgHandlerFunc[T]) {
	var v T
	msgID := receiver.messagePacket.GetRouter().GetMsgID(&v)
	if msgID == 0 {
		kklog.Error("message type not registered")
		return
	}
	h := NewMsgHandler[T](msgID, receiver.messagePacket.GetBodyCodec(), call)
	receiver.m[h.GetMsgID()] = h
}
