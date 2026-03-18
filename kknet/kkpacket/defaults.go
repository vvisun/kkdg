package kkpacket

import (
	"encoding/binary"
	"errors"
	"sync/atomic"
)

// [head]最多有几个part
const maxHeadPathCount = 8

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
func setByteOrder(order binary.ByteOrder) error {
	if order == nil {
		return errors.New("setByteOrder, order is nil")
	}
	if !gInitedByteOrder.CompareAndSwap(false, true) {
		return errors.New("setByteOrder, byte order already setted")
	}
	gByteOrder = order
	return nil
}

func getByteOrder() binary.ByteOrder {
	return gByteOrder
}

// 配置默认值。启动阶段初始化，运行期间不要修改。
//
//	@param streamTool 流拆解器
//	@param byteOrder 字节序
func ConfigDefaults(byteOrder binary.ByteOrder) error {
	return setByteOrder(byteOrder)
}
