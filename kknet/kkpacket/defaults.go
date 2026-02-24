package kkpacket

import (
	"encoding/binary"
)

// [head]最多有几个part
const maxHeadPathCount = 4

// 整包[length,message]最大长度（字节数）
var gMaxPacketSize = 2 * 1024

// 默认解包器
var defaultStreamPacket = NewLengthFieldStreamPacket(4)

func DefaultStreamPacket() IPacket {
	return defaultStreamPacket
}

func SetMaxPacketSize(size int) {
	gMaxPacketSize = size
}

// 整包[length,message]最大长度（字节数）
func MaxPacketSize() int {
	return gMaxPacketSize
}

var gByteOrder binary.ByteOrder = binary.BigEndian

func SetByteOrder(order binary.ByteOrder) {
	gByteOrder = order
}

func GetByteOrder() binary.ByteOrder {
	return gByteOrder
}
