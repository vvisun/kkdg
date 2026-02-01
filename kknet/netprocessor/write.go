package netprocessor

import (
	"time"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/queues/bbqueue"
)

type WriteOptions struct {
	SendQueueSize                 int                                           //发送队列大小
	SendQueueStrict               bool                                          //发送队列是否严格容量控制
	SendQueueNeedFlushOver        bool                                          //关闭时是否需要等待 flush 完成
	SendQueueTimeoutFlushOver     time.Duration                                 //关闭时等待 flush 完成的超时时间
	SendQueueFlushTimeoutCallback func(conn kknet.IConn, timeout time.Duration) //flush 超时回调
	WriteBatchSize                int                                           //每轮持锁时最多 Pop 的帧数，减少 Lock 次数与 Send 竞争
}

func CheckWriteOptions(opts *WriteOptions) {
	if opts == nil {
		return
	}
	if opts.SendQueueSize <= 0 {
		opts.SendQueueSize = 1024
	}
	if opts.WriteBatchSize <= 0 {
		opts.WriteBatchSize = 32
	}
}

/**
 * 消息处理器-发送器
 * 负责编码、然后将编码后的数据投入发送队列，供连接发送。
 */
type WriteProcessor struct {
	conn            kknet.IConn            //连接
	connID          kknet.CONN_ID          //连接ID，记录下来，方便conn关闭导致conn为空时，消费携程可以继续消费。
	userID          int64                  //用户ID，记录下来，方便业务逻辑层使用。记录conn绑定的用户ID。
	sendQueue       *bbqueue.BBQueue       //发送队列
	sendBatchBuffer []*kkbuffer.ByteBuffer //批量发送缓冲区
}

func NewWriteProcessor(opts WriteOptions) *WriteProcessor {
	CheckWriteOptions(&opts)
	return &WriteProcessor{
		sendQueue:       bbqueue.NewBBQueue(opts.SendQueueSize, opts.SendQueueStrict),
		sendBatchBuffer: make([]*kkbuffer.ByteBuffer, opts.WriteBatchSize),
	}
}

// 主动关闭时
func (wp *WriteProcessor) OnClose(conn kknet.IConn, err error) {

}

// 连接建立时
func (wp *WriteProcessor) OnConnect(conn kknet.IConn) {
	wp.conn = conn
	if conn == nil {
		return
	}
	go wp.writeLoop()
}

// 断线时
func (wp *WriteProcessor) OnDisconnect(conn kknet.IConn) {

}

func (wp *WriteProcessor) SendBuffer(buffer buffers.IBuffer) error {
	ok := wp.sendQueue.Push(buffer)
	if !ok {
		//发送队列已满，返回错误。
		//暂时直接返回，后续可以考虑丢弃/阻塞/...。
		return kkerrors.ErrSendQueueFull
	}
	wp.wakeWriter()
	return nil
}

// 发送消息。
func (wp *WriteProcessor) SendMessage(msg any) error {
	// 编码消息
	buffer, err := kkpacket.EncodeStream(msg, kkpacket.DefaultStreamPacket())
	if err != nil {
		return err
	}

	// 将数据放入发送队列
	ok := wp.sendQueue.Push(buffer)
	if !ok {
		//发送队列已满，返回错误。
		//暂时直接返回，后续可以考虑丢弃/阻塞/...。
		return kkerrors.ErrSendQueueFull
	}

	// 唤醒写携程，让写携程消费发送队列中的数据并发送。
	wp.wakeWriter()
	return nil
}

// 唤醒写携程。让写携程消费发送队列中的数据并发送。
func (wp *WriteProcessor) wakeWriter() {
	conn := wp.conn
	if conn == nil {
		return
	}
}

// 消费发送队列中的数据并发送。
func (wp *WriteProcessor) writeLoop() error {
	conn := wp.conn
	if conn == nil {
		return kkerrors.ErrConnNotSet
	}

	for {
		buffer := wp.sendQueue.Pop()
		if buffer == nil {
			break
		}
		// 这里可以调整为同步send，发送失败时重试/丢弃
		if err := conn.SendBuffer(buffer); err != nil {
			return err
		}
	}

	return nil
}
