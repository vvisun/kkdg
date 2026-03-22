package msgreceiver

import (
	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// NewMsgReceiver 创建消息接收器
func NewMsgReceiver(packetTool *kkpacket.FullPacket, decodeErrorCallback kkapp.DecodeErrorCallbackOnRaw) *MsgReceiver {
	return &MsgReceiver{
		packetTool:          packetTool,
		hdMap:               make(map[kkpacket.MSGID]IMsgHandler[kknet.CONN_ID]),
		decodeErrorCallback: decodeErrorCallback,
	}
}

// MsgReceiver 消息接收器
type MsgReceiver struct {
	packetTool          *kkpacket.FullPacket
	hdMap               map[kkpacket.MSGID]IMsgHandler[kknet.CONN_ID] // 消息ID到消息处理器的映射
	decodeErrorCallback kkapp.DecodeErrorCallbackOnRaw
}

var _ kknet.IRawHandler = (*MsgReceiver)(nil)

// OnRaw 实现kknet.IRawHandler接口。接收原始数据并分发到消息处理器。
//
//	用法1：直接将MsgReceiver当做IRawHandler作为kknet.Options的RawHandler选项。
//	用法2：自己创建IRawHandler实现，将MsgReceiver作为自定义IRawHandler的成员，在OnRaw方法中调用msgReceiver.OnRaw。
//
//	@param connId kknet.CONN_ID
//	@param bbPacket 原始数据 完整包[length,message]
func (r *MsgReceiver) OnRaw(connId kknet.CONN_ID, bbPacket *kkbuffer.ByteBuffer) {
	msgID, bodyBytes, err := parseMsgInfo(bbPacket.Bytes(), r.packetTool)
	if err != nil {
		kkbuffer.Put(bbPacket)
		// 解码错误，抛给上层处理，一般是客户端发来非法数据，可能是客户端版本过低，也可能是异常攻击。
		// 上层可以返回一个错误码给客户端，然后关闭连接，这样即对客户端友好，又能防止恶意攻击。
		if r.decodeErrorCallback != nil {
			r.decodeErrorCallback(connId, kkapp.GameErrorCodeDecodeError)
		}
		return
	}

	h, ok := r.hdMap[msgID]
	if !ok || h == nil {
		kkbuffer.Put(bbPacket)
		// 消息ID不存在，抛给上层处理，一般是客户端发来非法数据，可能是客户端版本过低，也可能是异常攻击。
		// 上层可以返回一个错误码给客户端，然后关闭连接，这样即对客户端友好，又能防止恶意攻击。
		if r.decodeErrorCallback != nil {
			r.decodeErrorCallback(connId, kkapp.GameErrorCodeMsgIDNotFound)
		}
		return
	}

	err = h.OnMessage(connId, bodyBytes, false)
	kkbuffer.Put(bbPacket)

	if err != nil {
		// 解码错误，抛给上层处理，一般是客户端发来非法数据，可能是客户端版本过低，也可能是异常攻击。
		// 上层可以返回一个错误码给客户端，然后关闭连接，这样即对客户端友好，又能防止恶意攻击。
		if r.decodeErrorCallback != nil {
			r.decodeErrorCallback(connId, kkapp.GameErrorCodeDecodeError)
		}
	}
}
