package kkpacket

import (
	"encoding/binary"
	"sync/atomic"

	"github.com/vvisun/kkdg/utils/kklog"
)

// [head]最多有几个part
const maxHeadPathCount = 8

// 默认解包器
var defaultStreamPacket = NewLengthFieldStreamPacket(4, 4*1024)

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
// 为了减少多余的心力花在对齐 各端间的流拆解器和字节序，导致编码解码不一致。
//
//	@param streamTool 流拆解器
//	@param byteOrder 字节序
func ConfigDefaults(byteOrder binary.ByteOrder) {
	if byteOrder != nil {
		setByteOrder(byteOrder)
	}
}
