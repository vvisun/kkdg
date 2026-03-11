package kkpacket

import (
	"encoding/binary"
	"sync/atomic"

	"github.com/vvisun/kkdg/utils/kklog"
)

// [head]最多有几个part
const maxHeadPathCount = 4

// 默认解包器
var (
	defaultStreamPacket       = NewLengthFieldStreamPacket(4, 4*1024)
	initedDefaultStreamPacket atomic.Bool
)

// 设置默认解包器。启动阶段初始化，运行期间不要修改。
// @param packet 解包器
func SetDefaultStreamPacket(packet IPacket) {
	if !initedDefaultStreamPacket.CompareAndSwap(false, true) {
		kklog.Warnf("[kknet] default stream packet already set")
		return
	}
	defaultStreamPacket = packet
}

func DefaultStreamPacket() IPacket {
	return defaultStreamPacket
}

var (
	gByteOrder       binary.ByteOrder = binary.BigEndian
	gInitedByteOrder atomic.Bool
)

// 设置字节序。启动阶段初始化，运行期间不要修改。
// @param order 字节序，bigEndian或littleEndian
func SetByteOrder(order binary.ByteOrder) {
	if !gInitedByteOrder.CompareAndSwap(false, true) {
		kklog.Warnf("[kknet] byte order already set")
		return
	}
	gByteOrder = order
}

func GetByteOrder() binary.ByteOrder {
	return gByteOrder
}
