package netprocessor

import (
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers"
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
	Start(conn kknet.IConn)
	Stop()
	EnqueuePacket(packet []byte)
	OnRecvBytes(data []byte) error
}

type IWriteProcessor interface {
	Start(conn kknet.IConn, writeFn WriteFunc, onWriteError func(error))
	Stop(err error)
	SendBuffer(buffer buffers.IBuffer) error
	SendMessage(msg any) error

	Pending() int          //测试在用
	Done() <-chan struct{} //测试在用
}

type WpProvider func(opts kknet.WriteOptions) IWriteProcessor
type RpProvider func(opts kknet.ReadOptions) IReadProcessor
