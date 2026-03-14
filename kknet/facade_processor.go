package kknet

import (
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

/*
*批量写入。WriteFunc中，发送失败的数据不释放，供调用方知道哪些数据发送失败。
 *@param batch 批量缓冲区，数组长度为 WriteOptions.WriteBatchSize
 *@param n 批量数量
 *@return error
*/
type WriteFunc func(batch []*kkbuffer.ByteBuffer, n int) error

type IReadProcessor interface {
	Start(conn IConn)
	Stop()
	EnqueuePacket(packet []byte)
	OnRecvBytes(data []byte) error
}

type IWriteProcessor interface {
	// @param conn 关联的连接(IConn)
	// @param writeFn 写数据函数。WriteFunc中，发送失败的数据不释放，供调用方知道哪些数据发送失败。
	// @param onWriteError 写数据错误回调。用于writeProcess发生致命错误时，通知IConn关闭连接。
	// @param stats 统计信息。用于统计写数据错误次数。目前是IConn里的Stats引用。
	Start(conn IConn, writeFn WriteFunc, onWriteError func(error), stats *Stats)
	// @param err 连接关闭的原因。
	// err为nil时，写处理器停止优雅：flush剩余数据，然后关闭连接。
	// err不为nil时，写处理器立即停止并关闭连接。
	Stop(err error)

	// @param buffer 要发送的数据（整包[length,message]）
	SendBuffer(buffer *kkbuffer.ByteBuffer) error
	// @param msg 要发送的数据（结构体对象）。writeProcessor内部会使用kkpacket编码。
	SendMsg(msg any) error

	Pending() int          //测试在用。返回当前队列中待发送的数据包数量。即：sendQueue.Len()。
	Done() <-chan struct{} //测试在用。返回写处理器停止的信号。
}

type WpProvider func(opts WriteOptions) IWriteProcessor
type RpProvider func(opts ReadOptions) IReadProcessor

type (
	// IRawHandler is a handler for raw data.
	IRawHandler interface {
		/*OnRaw is called when a raw data is received.
		@param connId CONN_ID 连接ID
		@param data *kkbuffer.ByteBuffer 原始数据
		@note 外部需记得释放buffer！！！否则buffer得不到回收，性能反而更低！！！
		@note 外部自行用解码器解码（内置的解码器见kkpacket）
		*/
		OnRaw(connId CONN_ID, data *kkbuffer.ByteBuffer)
	}

	// INoneCopyHandler is a handler for zero copy data.
	INoneCopyHandler interface {
		/*OnNoneCopy is called when a raw data is received.
		如果同步调用已经快过拷贝，可以直接同步消费数据，实现0拷贝优化。
		@param connId CONN_ID 连接ID
		@param data 为 slice，调用方不 Put，handler 不得保存 slice 引用. 如需保存，请自行拷贝。
		*/
		OnNoneCopy(connId CONN_ID, data []byte)
	}
)
