package kkpacket

import (
	"encoding/binary"

	"github.com/vvisun/kkdg/utils/kkcodec"
)

const defaultMaxMessageSize = 4 * 1024 //默认MaxMessageSize

var defaultStreamPacket = NewLengthFieldStreamPacket(
	NewPacketCodec(HeadTypeMid, kkcodec.GetCodec(kkcodec.CodecTypeJson)),
)

func DefaultStreamPacket() IStreamPacket {
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
