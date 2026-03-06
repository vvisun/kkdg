package kkapp

import (
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

// 网关与逻辑服之间的消息转发函数名
const (
	// 客户端->网关->逻辑服的消息转发函数名
	FuncNameC2S = "c2s"
	// 逻辑服->网关->客户端的消息转发函数名
	FuncNameSendToClient = "1"
	// 逻辑服->网关->多个客户端的消息转发函数名
	FuncNameSendToClients = "N"
)

// 网关与客户端之间的消息编码解码器
var gMsgPacket = kkpacket.NewMessagePacket(
	kkpacket.NewPacketHead(&kkpacket.PartUint32{}),
	kkcodec.GetCodec(kkcodec.CodecTypeJson),
	kkpacket.NewMsgRouter(),
)

// GetMsgPacket 获取网关与客户端之间的消息编码解码器
func GetMsgPacket() *kkpacket.MessagePacket {
	return gMsgPacket
}

// SetMsgPacket 设置网关与客户端之间的消息编码解码器
func SetMsgPacket(head *kkpacket.PacketHead, bodyCodec kkcodec.ICodec, router *kkpacket.MsgRouter) {
	gMsgPacket = kkpacket.NewMessagePacket(head, bodyCodec, router)
}
