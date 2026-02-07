package msgrouter

import (
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

type MsgHandlerFunc[T any] func(connId kknet.CONN_ID, msg *T) error

// 消息接收器
type IMsgHandler interface {
	GetMsgID() any                                      // 获取消息ID
	OnRaw(connId kknet.CONN_ID, bodyBytes []byte) error // 消息回调
}

type MsgHandler[T any] struct {
	call  MsgHandlerFunc[T]
	msgID any
	codec kkcodec.ICodec
}

var _ IMsgHandler = (*MsgHandler[any])(nil)

func (h *MsgHandler[T]) GetMsgID() any {
	return h.msgID
}

func (h *MsgHandler[T]) OnRaw(connId kknet.CONN_ID, bodyBytes []byte) error {
	var data T
	if err := h.codec.Unmarshal(bodyBytes, &data); err != nil {
		return err
	}
	err := h.call(connId, &data)
	if err != nil {
		return err
	}
	return nil
}

func NewMsgHandler[T any](msgID any, codec kkcodec.ICodec, call MsgHandlerFunc[T]) *MsgHandler[T] {
	var handler MsgHandler[T]
	handler.call = call
	handler.msgID = msgID
	handler.codec = codec
	return &handler
}
