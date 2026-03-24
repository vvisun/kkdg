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
	// Stop 停止接收新数据 和 消费完已接收的数据。
	// 新接收到的包视为待消费，投递到OnRaw/OnNoneCopy视为某条消息已消费完毕。
	//  具体实现:
	//  ReadProcessor 会等待内部【消费携程】消费完所有【待消费数据队列】中的数据；
	//  SyncReadProcessor 因为是同步处理，没有异步队列，所以只需等待当前正在执行的 OnNoneCopy 退出临界区；
	//  WorkerReadProcessor 会直接投递给WorkerQueue。
	Stop()
	// 收到单个完整包数据时（生产者生产数据）。eg: gnet SplitSR 得到完整单包 [length,message]。
	// 仅由网络读协程访问。
	EnqueuePacket(packet []byte) error
	// 收到数据时（生产者生产数据）。
	// 仅由网络读协程访问。
	// 不确定收到的是单个完整包还是多个包时的通用方法。
	OnRecvBytes(data []byte) error
	// 返回当前内部待处理队列长度。
	// ReadProcessor = recvQueue.Len()；WorkerReadProcessor = workQueue.Len()；SyncReadProcessor 因无异步队列固定为 0。
	// RecvQueueStrict 的限流语义与此保持一致。
	Pending() int
}

type IWriteProcessor interface {
	// @param conn 关联的连接(IConn)
	// @param writeFn 写数据函数。WriteFunc中，发送失败的数据不释放，供调用方知道哪些数据发送失败。
	// @param onWriteError 写数据错误回调。用于writeProcess发生致命错误时，通知IConn关闭连接。
	// @param stats 统计信息。用于统计写数据错误次数。目前是IConn里的Stats引用。
	Start(conn IConn, writeFn WriteFunc, onWriteError func(error), stats *Stats)
	// @param err 连接关闭的原因。
	// err为nil时，写处理器优雅停止：flush剩余数据，然后关闭连接。
	// err不为nil时，写处理器立即停止并关闭连接。
	Stop(err error)

	// @param buffer 要发送的数据（整包[length,message]）
	SendBuffer(buffer *kkbuffer.ByteBuffer) error
	// @param msg 要发送的数据（结构体对象）。writeProcessor内部会使用kkpacket编码。
	// 前提：opts.MsgPacket 必须已正确配置；未配置时属于必现的配置错误，当前实现允许直接 panic。
	SendMsg(msg any) error

	//返回当前队列中待发送的数据包数量。即：sendQueue.Len()。
	Pending() int

	//测试中用到。返回写处理器停止的信号。
	Done() <-chan struct{}
}

type WpProvider func(opts WriteOptions) IWriteProcessor
type RpProvider func(opts ReadOptions) IReadProcessor

type (
	// IRawHandler is a handler for raw data.
	IRawHandler interface {
		/*OnRaw is called when a raw data is received.
		@param connId CONN_ID 连接ID
		@param bbPacket *kkbuffer.ByteBuffer 原始数据[length,message]
		@note 外部需记得释放buffer！！！否则buffer得不到回收，性能反而更低！！！
		@note 为了防止静默吞避业务层致命错误，网络层并未用safe call，外部需自行处理panic。
		@note 由于连接可能已经关闭，IConn可能已经为nil，外部可以通过connId从IConnManager中获取连接，nil情况自行处理。
		*/
		OnRaw(connId CONN_ID, bbPacket *kkbuffer.ByteBuffer)
	}

	// INoneCopyHandler is a handler for zero copy data.
	INoneCopyHandler interface {
		/*OnNoneCopy is called when a raw data is received.
		如果同步调用已经快过拷贝，可以直接同步消费数据，实现0拷贝优化。
		@param connId CONN_ID 连接ID
		@param packet 整包数据[length,message]。不得保存 packet 引用，如需保存，请自行拷贝。
		@note 为了防止静默吞避业务层致命错误，网络层并未用safe call，外部需自行处理panic。
		@note 由于连接可能已经关闭，IConn可能已经为nil，外部可以通过connId从IConnManager中获取连接，nil情况自行处理。
		*/
		OnNoneCopy(connId CONN_ID, packet []byte)
	}
)
