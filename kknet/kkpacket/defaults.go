package kkpacket

import (
	"encoding/binary"

	"github.com/vvisun/kkdg/utils/kkcodec"
)

const defaultMaxMessageSize = 4 * 1024 //默认MaxMessageSize

var defaultStreamPacket = NewLengthFieldStreamPacket(
	4,                                       // [length]部分的字节数。该部分用于表示包体[message]的长度。
	NewPacketHead(&PartUint32{}),            // msgId
	kkcodec.GetCodec(kkcodec.CodecTypeJson), // 消息体编码器。用于编码解码[body]部分。
)

func DefaultStreamPacket() IPacket {
	return defaultStreamPacket
}

// 整包最大长度，包括长度字段。[length,message]
func DefaultMaxMessageSize() int {
	return defaultMaxMessageSize
}

var gByteOrder binary.ByteOrder = binary.BigEndian

func SetByteOrder(order binary.ByteOrder) {
	gByteOrder = order
}

func GetByteOrder() binary.ByteOrder {
	return gByteOrder
}
