package kkapp

import (
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

// 网关与客户端之间的消息路由器
var gMsgRouter = kkpacket.NewMsgRouter()

// 网关与客户端之间的消息编码解码器
var gMsgPacket = kkpacket.NewMessagePacket(
	kkpacket.NewPacketHead(&kkpacket.PartUint32{}),
	kkcodec.GetCodec(kkcodec.CodecTypeJson),
	gMsgRouter,
)

// GetMsgPacket 获取网关与客户端之间的消息编码解码器
func GetMsgPacket() *kkpacket.MessagePacket {
	return gMsgPacket
}
