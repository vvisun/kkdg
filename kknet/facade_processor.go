package kknet

import (
	"github.com/vvisun/kkdg/kknet/kkpacket"
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
	Start(conn IConn, writeFn WriteFunc, onWriteError func(error))
	Stop(err error)
	SendBuffer(buffer *kkbuffer.ByteBuffer) error
	SendMessage(msg any) error

	Pending() int          //测试在用
	Done() <-chan struct{} //测试在用
}

type WpProvider func(opts WriteOptions) IWriteProcessor
type RpProvider func(opts ReadOptions) IReadProcessor

type (
	// IMsgHandler is a handler for messages.
	IMsgHandler interface {
		/*OnMsg is called when a message is received.
		@param connId CONN_ID 连接ID
		@param msg any 消息对象（object）
		@param msgID kkpacket.MSGID 消息ID
		@note 外部需记得释放消息对象！！！否则消息对象得不到回收，性能反而更低！！！
		@note 外部自行用解码器解码（内置的解码器见kkpacket/parser.go）
		*/
		OnMsg(connId CONN_ID, msg any, msgID kkpacket.MSGID)
	}

	// IRawHandler is a handler for raw data.
	IRawHandler interface {
		/*OnRaw is called when a raw data is received.
		@param connId CONN_ID 连接ID
		@param data *kkbuffer.ByteBuffer 原始数据
		@note 外部需记得释放buffer！！！否则buffer得不到回收，性能反而更低！！！
		@note 外部自行用解码器解码（内置的解码器见kkpacket/parser.go）
		*/
		OnRaw(connId CONN_ID, data *kkbuffer.ByteBuffer)
	}

	// INoneCopyHandler is a handler for zero copy data.
	INoneCopyHandler interface {
		/*OnNoneCopy is called when a raw data is received.
		如果同步调用已经快过拷贝，可以直接同步消费数据，实现0拷贝优化。
		@param connId CONN_ID 连接ID
		@param data *kkbuffer.ByteBuffer 原始数据
		@note 外部需记得释放buffer！！！否则buffer得不到回收，性能反而更低！！！
		*/
		OnNoneCopy(connId CONN_ID, data []byte)
	}
)
