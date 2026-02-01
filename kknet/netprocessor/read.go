package netprocessor

import (
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/queues/bbqueue"
)

type ReadOptions struct {
	RecvQueueSize   int                                                //接收队列大小
	RecvQueueStrict bool                                               //接收队列是否严格容量控制
	NeedDecode      bool                                               //是否需要解码
	ConsumerMsgFunc func(c kknet.IConn, msg any, msgID kkpacket.MSGID) //消费函数
	ConsumerRawFunc func(c kknet.IConn, data buffers.IBuffer)          //消费函数
}

func CheckReadOptions(opts *ReadOptions) {
	if opts == nil {
		return
	}
	if opts.RecvQueueSize <= 0 {
		opts.RecvQueueSize = 1024
	}
}

/**
 * 消息处理器-接收器
 * 负责从连接中读取数据，并将其放入接收队列中。
 * 接收队列中的数据可以被其他组件消费。
 */
type ReadProcessor struct {
	conn      kknet.IConn      //连接
	recvBuf   []byte           //接收缓冲区
	recvQueue *bbqueue.BBQueue //接收队列
	opts      ReadOptions      //选项
}

func NewReadProcessor(opts ReadOptions) *ReadProcessor {
	CheckReadOptions(&opts)
	return &ReadProcessor{
		recvBuf:   make([]byte, 0, 1024*1024), //1024KB
		recvQueue: bbqueue.NewBBQueue(opts.RecvQueueSize, opts.RecvQueueStrict),
		opts:      opts,
	}
}

// 主动关闭时
func (rp *ReadProcessor) OnClose(conn kknet.IConn, err error) {

}

// 连接建立时
func (rp *ReadProcessor) OnConnect(conn kknet.IConn) {
	rp.conn = conn
	if conn == nil {
		return
	}
	go rp.consumeRecvQueue()
}

// 断线时
func (rp *ReadProcessor) OnDisconnect(conn kknet.IConn) {

}

// 收到数据时（生产者生产数据）
func (rp *ReadProcessor) OnRecvBytes(data []byte) {
	rp.recvBuf = append(rp.recvBuf, data...)
	// 粘包拆包
	packets, err := kkpacket.DefaultStreamPacket().Split(rp.recvBuf, nil)
	if err != nil {
		return
	}
	rp.recvBuf = rp.recvBuf[:0]
	for _, packet := range packets {
		rp.recvQueue.Push(packet)
	}
	// 唤醒消费携程，消费recvQueue中的数据。
	rp.wakeConsumer()
}

// 唤醒消费携程。
func (rp *ReadProcessor) wakeConsumer() {

}

// 消费携程：消费recvQueue中的数据，并分发消息。
func (rp *ReadProcessor) consumeRecvQueue() {
	for {
		packet := rp.recvQueue.Pop()
		if packet == nil {
			break
		}
		msg, msgID, err := kkpacket.DecodeStream(packet.B, kkpacket.DefaultStreamPacket())
		kkbuffer.Put(packet)
		if err != nil {
			continue
		}
		rp.dispatchMessage(msg, msgID)
	}
}

// 分发消息到业务逻辑层
func (rp *ReadProcessor) dispatchMessage(msg any, msgID kkpacket.MSGID) {
	// call handler.OnMessage
}
