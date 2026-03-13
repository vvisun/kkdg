package kkapp

import (
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

var (
	gStreamTool kkpacket.IPacket = kkpacket.DefaultStreamPacket()
)

// 获取默认的流拆解器
func GetStreamTool() kkpacket.IPacket {
	return gStreamTool
}

// 网关与客户端之间的消息编码解码器
var gClientMsgPacket = kkpacket.NewMessagePacket(
	kkpacket.NewPacketHead(&kkpacket.PartUint32{}),
	kkcodec.GetCodec(kkcodec.CodecTypeJson),
	kkpacket.NewMsgRouter(),
)

// GetClientMsgPacket 获取网关与客户端之间的消息编码解码器
func GetClientMsgPacket() *kkpacket.MessagePacket {
	return gClientMsgPacket
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
// 为了减少多余的心力花在对齐 网关，客户端，业务服三者间的编码解码器，导致编码解码不一致。
//
//	@param msgPacket 网关与客户端之间的消息编码解码器
//	@param transMsgPacket 网关与业务服之间的消息编码解码器
func ConfigDefaults(streamTool kkpacket.IPacket, msgPacket *kkpacket.MessagePacket, transMsgPacket *kkpacket.MessagePacket) {
	if streamTool != nil {
		gStreamTool = streamTool
	}
	if msgPacket != nil {
		gClientMsgPacket = msgPacket
	}
	if transMsgPacket != nil {
		gTransMsgPacket = transMsgPacket
	}
}
