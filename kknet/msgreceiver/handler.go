package msgreceiver

import (
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

type MsgHandlerFunc[T any, K any] func(connId K, msg *T) error

// 消息接收器
type IMsgHandler[K any] interface {
	GetMsgID() kkpacket.MSGID               // 获取消息ID
	OnRaw(connId K, bodyBytes []byte) error // 消息回调
}

type MsgHandler[T any, K any] struct {
	call  MsgHandlerFunc[T, K] // 消息回调
	msgID kkpacket.MSGID       // 消息ID
	codec kkcodec.ICodec       // 消息编码器
}

var _ IMsgHandler[any] = (*MsgHandler[any, any])(nil)

func (h *MsgHandler[T, K]) GetMsgID() kkpacket.MSGID {
	return h.msgID
}

func (h *MsgHandler[T, K]) OnRaw(connId K, bodyBytes []byte) error {
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

func NewMsgHandler[T any, K any](msgID kkpacket.MSGID, codec kkcodec.ICodec, call MsgHandlerFunc[T, K]) *MsgHandler[T, K] {
	var handler MsgHandler[T, K]
	handler.call = call
	handler.msgID = msgID
	handler.codec = codec
	return &handler
}
