package kkapp

import (
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

// 网关与业务服之间的消息转发函数名
const (
	// 客户端->网关->业务服的消息转发函数名
	FuncNameC2S = "c2s"
	// 业务服->网关->客户端的消息转发函数名
	FuncNameSendToClient = "1"
	// 业务服->网关->多个客户端的消息转发函数名
	FuncNameSendToClients = "N"
	// 网关 -> 业务服：客户端断开事件
	FuncNameClientDisconnect = "cliMiss"
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

// 网关与业务服之间的消息编码解码器
var gTransMsgPacket = kkpacket.NewMessagePacket(
	kkpacket.NewPacketHead(&kkpacket.PartUint32{}),
	kkcodec.GetCodec(kkcodec.CodecTypeMsgpack),
	kkpacket.NewMsgRouter(),
)

// GetTransMsgPacket 获取网关与业务服之间的消息编码解码器
func GetTransMsgPacket() *kkpacket.MessagePacket {
	return gTransMsgPacket
}

// 配置默认值。启动阶段初始化，运行期间不要修改。
// @param msgPacket 网关与客户端之间的消息编码解码器
// @param transMsgPacket 网关与业务服之间的消息编码解码器
func ConfigDefaults(msgPacket *kkpacket.MessagePacket, transMsgPacket *kkpacket.MessagePacket) {
	if msgPacket != nil {
		gMsgPacket = msgPacket
	}
	if transMsgPacket != nil {
		gTransMsgPacket = transMsgPacket
	}
}
