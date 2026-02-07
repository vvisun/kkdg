package msgrouter

import (
	"reflect"

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

// RegistMsgHandler 注册消息处理器
func RegistMsgHandler[T any](receiver *MsgReceiver, h *MsgHandler[T]) {
	if h == nil {
		return
	}
	receiver.m[h.GetMsgID()] = h
}

func RegisterMsgHandler[T any](receiver *MsgReceiver, call MsgHandlerFunc[T]) {
	msgID := receiver.messagePacket.GetRouter().GetMsgID(new(T))
	if msgID == 0 {
		kklog.Errorf("message type %v is not registered", reflect.TypeOf(new(T)))
		return
	}
	handler := NewMsgHandler[T](msgID, receiver.messagePacket.GetBodyCodec(), call)
	RegistMsgHandler(receiver, handler)
}
