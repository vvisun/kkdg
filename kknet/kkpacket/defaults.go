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

// 完整包工具。流拆解器 + 消息编码解码器
type FullPacket struct {
	streamTool  IPacket        //流拆解器
	messageTool *MessagePacket //消息编码解码器
}

// 完整包工具。
// @param streamTool 流拆解器
// @param messageTool 消息编码解码器
func NewFullPacket(streamTool IPacket, messageTool *MessagePacket) *FullPacket {
	if streamTool == nil {
		panic("streamTool is nil")
	}
	if messageTool == nil {
		panic("messageTool is nil")
	}
	return &FullPacket{
		streamTool:  streamTool,
		messageTool: messageTool,
	}
}

func (f *FullPacket) GetStreamTool() IPacket {
	return f.streamTool
}

func (f *FullPacket) GetMessageTool() *MessagePacket {
	return f.messageTool
}
