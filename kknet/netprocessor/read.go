package netprocessor

import (
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/queues/bbqueue"
)

/**
 * 消息处理器-接收器
 * 负责从连接中读取数据，并将其放入接收队列中。
 * 接收队列中的数据可以被其他组件消费。
 */
type ReadProcessor struct {
	conn      kknet.IConn      //连接
	recvBuf   []byte           //接收缓冲区
	recvQueue *bbqueue.BBQueue //接收队列
}

func NewReadProcessor() *ReadProcessor {
	return &ReadProcessor{
		recvBuf:   make([]byte, 0, 1024*1024), //1024KB
		recvQueue: bbqueue.NewBBQueue(1024, false),
	}
}

// 主动关闭时
func (np *ReadProcessor) OnClose() {

}

// 连接建立时
func (np *ReadProcessor) OnConnected(conn kknet.IConn) {
	np.conn = conn
	if conn == nil {
		return
	}
	go np.consumeRecvQueue()
}

// 断线时
func (np *ReadProcessor) OnDisconnect() {

}

// 收到数据时
func (np *ReadProcessor) OnRecvBytes(data []byte) {
	np.recvBuf = append(np.recvBuf, data...)
	// 粘包拆包
	packets, err := kkpacket.DefaultStreamPacket().Split(np.recvBuf, nil)
	if err != nil {
		return
	}
	np.recvBuf = np.recvBuf[:0]
	for _, packet := range packets {
		np.recvQueue.Push(packet)
	}
}

// 消费携程：消费接收队列中的数据，并分发消息。
func (np *ReadProcessor) consumeRecvQueue() {
	for {
		packet := np.recvQueue.Pop()
		if packet == nil {
			break
		}
		msg, msgID, err := kkpacket.DecodeStream(packet.B, kkpacket.DefaultStreamPacket())
		kkbuffer.Put(packet)
		if err != nil {
			continue
		}
		np.dispatchMessage(msg, msgID)
	}
}

// 分发消息到业务逻辑层
func (np *ReadProcessor) dispatchMessage(msg any, msgID kkpacket.MSGID) {
	// call handler.OnMessage
}
