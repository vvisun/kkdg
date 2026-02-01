package netprocessor

import (
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/queues/bbqueue"
)

const writeBatchSize = 32 // 每轮持锁时最多 Pop 的帧数，减少 Lock 次数与 Send 竞争

/**
 * 消息处理器-发送器
 * 负责编码、然后将编码后的数据投入发送队列，供连接发送。
 */
type WriteProcessor struct {
	conn            kknet.IConn            //连接
	sendQueue       *bbqueue.BBQueue       //发送队列
	sendBatchBuffer []*kkbuffer.ByteBuffer //批量发送缓冲区
}

func NewWriteProcessor() *WriteProcessor {
	return &WriteProcessor{
		sendQueue:       bbqueue.NewBBQueue(1024, false),
		sendBatchBuffer: make([]*kkbuffer.ByteBuffer, writeBatchSize),
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
		// 这里可以优化为同步send，发送失败时重试/丢弃
		if err := conn.SendBuffer(buffer); err != nil {
			return err
		}
	}

	return nil
}
