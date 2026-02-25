package msgreceiver

import (
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

type MsgHandlerFunc[T any] func(connId kknet.CONN_ID, msg *T) error

// 消息接收器
type IMsgHandler interface {
	GetMsgID() kkpacket.MSGID                           // 获取消息ID
	OnRaw(connId kknet.CONN_ID, bodyBytes []byte) error // 消息回调
}

type MsgHandler[T any] struct {
	call  MsgHandlerFunc[T] // 消息回调
	msgID kkpacket.MSGID    // 消息ID
	codec kkcodec.ICodec    // 消息编码器
}

var _ IMsgHandler = (*MsgHandler[any])(nil)

func (h *MsgHandler[T]) GetMsgID() kkpacket.MSGID {
	return h.msgID
}

func (h *MsgHandler[T]) OnRaw(connId kknet.CONN_ID, bodyBytes []byte) error {
	var data T
	if err := h.codec.Unmarshal(bodyBytes, &data); err != nil {
		return err
	}
	err := h.call(connId, &data) // 调用消息回调
	if err != nil {
		return err
	}
	return nil
}

func NewMsgHandler[T any](msgID kkpacket.MSGID, codec kkcodec.ICodec, call MsgHandlerFunc[T]) *MsgHandler[T] {
	var handler MsgHandler[T]
	handler.call = call
	handler.msgID = msgID
	handler.codec = codec
	return &handler
}
