package msgreceiver

import (
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// NewMsgReceiver 创建消息接收器
func NewMsgReceiver[K any](packetTool *kkpacket.FullPacket) *MsgReceiver[K] {
	return &MsgReceiver[K]{
		packetTool: packetTool,
		hdMap:      make(map[kkpacket.MSGID]IMsgHandler[K]),
	}
}

// MsgReceiver 消息接收器
type MsgReceiver[K any] struct {
	packetTool *kkpacket.FullPacket
	hdMap      map[kkpacket.MSGID]IMsgHandler[K] // 消息ID到消息处理器的映射
}

var _ kknet.IRawHandler = (*MsgReceiver[kknet.CONN_ID])(nil)

// OnRaw 实现kknet.IRawHandler接口。接收原始数据并分发到消息处理器。
//
//	用法1：直接将MsgReceiver当做IRawHandler作为kknet.Options的RawHandler选项。
//	用法2：自己创建IRawHandler实现，将MsgReceiver作为自定义IRawHandler的成员，在OnRaw方法中调用msgReceiver.OnRaw。
//
//	@param connId kknet.CONN_ID
//	@param bbPacket 原始数据 完整包[length,message]
func (r *MsgReceiver[K]) OnRaw(connId K, bbPacket *kkbuffer.ByteBuffer) {
	msgID, bodyBytes, err := parseMsgInfo(bbPacket.Bytes(), r.packetTool)
	if err != nil {
		kkbuffer.Put(bbPacket)
		return
	}

	h, ok := r.hdMap[msgID]
	if !ok || h == nil {
		kkbuffer.Put(bbPacket)
		return
	}

	h.OnMessage(connId, bodyBytes)
	kkbuffer.Put(bbPacket)
}
