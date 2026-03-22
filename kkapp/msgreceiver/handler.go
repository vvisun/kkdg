package msgreceiver

import (
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/byteslice"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

// 消息回调函数。外部注册进来的消息处理函数
//
//	@param connKey kknet.CONN_ID或sessionID
//	@param msg 消息体
type MsgHandlerFunc[K comparable, T any] func(connKey K, msg *T)

// 消息接收器
type IMsgHandler[K comparable] interface {
	// 获取消息ID
	GetMsgID() kkpacket.MSGID
	// 消息回调。
	//  @param connKey kknet.CONN_ID或sessionID
	//  @param bodyBytes 消息体二进制数据。
	//  @return error 解码错误
	OnMessage(connKey K, bodyBytes []byte, needRelease bool) error
}

type MsgHandler[K comparable, T any] struct {
	call  MsgHandlerFunc[K, T] // 消息回调。外部注册进来的消息处理函数
	msgID kkpacket.MSGID       // 消息ID
	codec kkcodec.ICodec       // 消息编码器
}

var _ IMsgHandler[any] = (*MsgHandler[any, any])(nil)

// implements IMsgHandler.GetMsgID
func (h *MsgHandler[K, T]) GetMsgID() kkpacket.MSGID {
	return h.msgID
}

// implements IMsgHandler.OnMessage
//
//	@param connKey kknet.CONN_ID或sessionID
//	@param bodyBytes 消息体二进制数据。
//	@param needRelease 是否需要释放bodyBytes。
//	@return error 解码错误
func (h *MsgHandler[K, T]) OnMessage(connKey K, bodyBytes []byte, needRelease bool) error {
	var data T
	if err := h.codec.Unmarshal(bodyBytes, &data); err != nil {
		byteslice.Put(bodyBytes)
		return err
	}
	if needRelease {
		byteslice.Put(bodyBytes)
	}
	// 调用消息回调. 外部注册进来的消息处理函数
	h.call(connKey, &data)
	return nil
}

func newMsgHandler[K comparable, T any](msgID kkpacket.MSGID, codec kkcodec.ICodec, call MsgHandlerFunc[K, T]) *MsgHandler[K, T] {
	var handler MsgHandler[K, T]
	handler.call = call
	handler.msgID = msgID
	handler.codec = codec
	return &handler
}
