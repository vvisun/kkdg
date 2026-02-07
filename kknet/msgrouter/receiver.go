package msgrouter

import (
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// MsgReceiver 消息接收器
type MsgReceiver struct {
	m map[interface{}]IMsgHandler // 消息ID到消息处理器的映射
}

// OnRaw 接收原始数据并分发到消息处理器
func (r *MsgReceiver) OnRaw(connId kknet.CONN_ID, data *kkbuffer.ByteBuffer) error {
	messageBytes, err := kkpacket.DefaultStreamPacket().Unpack(data.Bytes())
	if err != nil {
		kkbuffer.Put(data)
		return err
	}

	msgID, msgBytes, err := kkpacket.ParseMsgInfo(messageBytes, kkpacket.DefaultStreamPacket().GetMessageCodec())
	if err != nil {
		kkbuffer.Put(data)
		return err
	}

	h, ok := r.m[msgID]
	if !ok || h == nil {
		return kkerrors.ErrMsgHandlerNotRegistered
	}
	err = h.OnRaw(connId, msgBytes)
	kkbuffer.Put(data)
	if err != nil {
		return err
	}
	return nil
}

// NewMsgReceiver 创建消息接收器
func NewMsgReceiver() *MsgReceiver {
	return &MsgReceiver{
		m: make(map[interface{}]IMsgHandler),
	}
}

// RegistMsgHandler 注册消息处理器
func RegistMsgHandler[T any](receiver *MsgReceiver, h *MsgHandler[T]) {
	if h == nil {
		return
	}
	receiver.m[h.GetMsgID()] = h
}
