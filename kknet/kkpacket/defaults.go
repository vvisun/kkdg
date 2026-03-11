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
func setDefaultStreamPacket(packet IPacket) {
	if packet == nil {
		kklog.Warn("[kknet] setDefaultStreamPacket packet is nil, ignore")
		return
	}
	if !initedDefaultStreamPacket.CompareAndSwap(false, true) {
		kklog.Warnf("[kknet] default stream packet already setted, ignore")
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
func setByteOrder(order binary.ByteOrder) {
	if order == nil {
		kklog.Warn("[kknet] setByteOrder order is nil, ignore")
		return
	}
	if !gInitedByteOrder.CompareAndSwap(false, true) {
		kklog.Warnf("[kknet] byte order already setted, ignore")
		return
	}
	gByteOrder = order
}

func GetByteOrder() binary.ByteOrder {
	return gByteOrder
}

// 配置默认值。启动阶段初始化，运行期间不要修改。
// @param streamTool 流拆解器
// @param byteOrder 字节序
func ConfigDefaults(streamTool IPacket, byteOrder binary.ByteOrder) {
	if streamTool != nil {
		setDefaultStreamPacket(streamTool)
	}
	if byteOrder != nil {
		setByteOrder(byteOrder)
	}
}
