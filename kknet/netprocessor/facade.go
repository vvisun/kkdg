package netprocessor

import (
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

/*
*批量携入
 *@param batch 批量缓冲区，数组长度为 WriteBatchSize
 *@param n 批量数量
 *@return 发送失败的数据序列，error
*/
type WriteFunc func(batch []*kkbuffer.ByteBuffer, n int) ([]*kkbuffer.ByteBuffer, error)

type IReadProcessor interface {
	Start(conn kknet.IConn)
	Stop()
	EnqueuePacket(packet []byte)
	OnRecvBytes(data []byte) error
}

type IWriteProcessor interface {
	Start(conn kknet.IConn, writeFn WriteFunc, onWriteError func(error))
	Stop()
	SendBuffer(buffer buffers.IBuffer) error
	SendMessage(msg any) error
}
