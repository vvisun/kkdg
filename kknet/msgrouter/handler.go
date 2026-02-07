package msgrouter

import (
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

// 消息接收器
type IMsgHandler interface {
	GetMsgID() any                                     // 获取消息ID
	OnRaw(connId kknet.CONN_ID, msgBytes []byte) error // 消息回调
}

type MsgHandler[T any] struct {
	call  func(connId kknet.CONN_ID, msg *T) error
	msgID any
	codec kkcodec.ICodec
}

var _ IMsgHandler = (*MsgHandler[any])(nil)

func (h *MsgHandler[T]) GetMsgID() any {
	return h.msgID
}

func (h *MsgHandler[T]) OnRaw(connId kknet.CONN_ID, msgBytes []byte) error {
	var data T
	if err := h.codec.Unmarshal(msgBytes, &data); err != nil {
		return err
	}
	err := h.call(connId, &data)
	if err != nil {
		return err
	}
	return nil
}

func NewMsgHandler[T any](msgID any, codec kkcodec.ICodec, call func(connId kknet.CONN_ID, msg *T) error) *MsgHandler[T] {
	var handler MsgHandler[T]
	handler.call = call
	handler.msgID = msgID
	handler.codec = codec
	return &handler
}
