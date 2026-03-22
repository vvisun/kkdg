package msgreceiver

import (
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/byteslice"
	"github.com/vvisun/kkdg/utils/kkcodec"
	"github.com/vvisun/kkdg/utils/kklog"
)

// 消息回调函数。外部注册进来的消息处理函数
type MsgHandlerFunc[T any, K any] func(connKey K, msg *T) error

// 消息接收器
type IMsgHandler[K any] interface {
	// 获取消息ID
	GetMsgID() kkpacket.MSGID
	// 消息回调。
	//  @param connKey kknet.CONN_ID或sessionID
	//  @param bodyBytes 消息体二进制数据。
	//  @return error 错误
	OnMessage(connKey K, bodyBytes []byte) error
}

type MsgHandler[T any, K any] struct {
	call  MsgHandlerFunc[T, K] // 消息回调。外部注册进来的消息处理函数
	msgID kkpacket.MSGID       // 消息ID
	codec kkcodec.ICodec       // 消息编码器
}

var _ IMsgHandler[any] = (*MsgHandler[any, any])(nil)

// implements IMsgHandler.GetMsgID
func (h *MsgHandler[T, K]) GetMsgID() kkpacket.MSGID {
	return h.msgID
}

// implements IMsgHandler.OnMessage
//
//	@param connKey kknet.CONN_ID或sessionID
//	@param bodyBytes 消息体二进制数据。
//	@return error 错误
func (h *MsgHandler[T, K]) OnMessage(connKey K, bodyBytes []byte) error {
	var data T
	if err := h.codec.Unmarshal(bodyBytes, &data); err != nil {
		byteslice.Put(bodyBytes)
		return err
	}
	byteslice.Put(bodyBytes)
	// 调用消息回调. 外部注册进来的消息处理函数
	err := h.call(connKey, &data)
	if err != nil {
		return err
	}
	return nil
}

func newMsgHandler[T any, K any](msgID kkpacket.MSGID, codec kkcodec.ICodec, call MsgHandlerFunc[T, K]) *MsgHandler[T, K] {
	var handler MsgHandler[T, K]
	handler.call = call
	handler.msgID = msgID
	handler.codec = codec
	return &handler
}

// RegisterMsgHandler 注册消息处理器
// 非线程安全，一般在初始化时调用，故不考虑线程安全
func RegisterMsgHandler[T any, K any](receiver *MsgReceiver[K], call MsgHandlerFunc[T, K]) {
	var v T
	msgID := receiver.packetTool.GetMessageTool().GetRouter().GetMsgID(&v)
	if msgID == 0 {
		kklog.Error("message type not registered")
		return
	}
	h := newMsgHandler[T, K](msgID, receiver.packetTool.GetMessageTool().GetBodyCodec(), call)
	receiver.hdMap[msgID] = h
}

func RegisterSessionMsgHandler[T any, K any](receiver *SessionMsgReceiver[K], call MsgHandlerFunc[T, K]) {
	var v T
	msgID := receiver.packetTool.GetMessageTool().GetRouter().GetMsgID(&v)
	if msgID == 0 {
		kklog.Error("message type not registered")
		return
	}
	h := newMsgHandler[T, K](msgID, receiver.packetTool.GetMessageTool().GetBodyCodec(), call)
	receiver.hdMap[msgID] = h
}
