package netprocessor

import (
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/queues/bbqueue"
)

type ReadOptions struct {
	RecvQueueSize   int                                                  //接收队列大小
	RecvQueueStrict bool                                                 //接收队列是否严格容量控制
	NeedDecode      bool                                                 //是否需要解码
	MsgHandler      func(c kknet.CONN_ID, msg any, msgID kkpacket.MSGID) //消费函数
	RawHandler      func(c kknet.CONN_ID, data buffers.IBuffer)          //消费函数
	RecvBatchSize   int                                                  //每轮消费最多 Pop 的帧数
}

func CheckReadOptions(opts *ReadOptions) {
	if opts == nil {
		return
	}
	if opts.RecvQueueSize <= 0 {
		opts.RecvQueueSize = 1024
	}
	if opts.RecvBatchSize <= 0 {
		opts.RecvBatchSize = 32
	}
}

/**
 * 消息处理器-接收器
 * 负责从连接中读取数据，并将其放入接收队列中。
 * 接收队列中的数据可以被其他组件消费。
 */
type ReadProcessor struct {
	conn      kknet.IConn      //连接
	connID    kknet.CONN_ID    //连接ID，记录下来，方便conn关闭导致conn为空时，消费携程可以继续消费。
	userID    int64            //用户ID，记录下来，方便业务逻辑层使用。记录conn绑定的用户ID。
	recvBuf   []byte           //接收缓冲区
	recvQueue *bbqueue.BBQueue //接收队列
	opts      ReadOptions      //选项

	mu        sync.Mutex
	closeOnce sync.Once
	closing   atomic.Bool
	wakeCh    chan struct{}
	closeCh   chan struct{}
	doneCh    chan struct{}
	batchBuf  []*kkbuffer.ByteBuffer
}

func NewReadProcessor(opts ReadOptions) *ReadProcessor {
	CheckReadOptions(&opts)
	return &ReadProcessor{
		recvBuf:   make([]byte, 0, 1024*1024), //1024KB
		recvQueue: bbqueue.NewBBQueue(opts.RecvQueueSize, opts.RecvQueueStrict),
		opts:      opts,
		wakeCh:    make(chan struct{}, 1),
		closeCh:   make(chan struct{}),
		doneCh:    make(chan struct{}),
		batchBuf:  make([]*kkbuffer.ByteBuffer, opts.RecvBatchSize),
	}
}

func (rp *ReadProcessor) Done() <-chan struct{} { return rp.doneCh }

// 连接建立时 / 启动消费协程
func (rp *ReadProcessor) Start(conn kknet.IConn) {
	rp.conn = conn
	if conn == nil {
		return
	}
	rp.connID = conn.ID()
	go rp.consumeRecvQueue()
}

func (rp *ReadProcessor) Stop() {
	rp.closeOnce.Do(func() {
		rp.closing.Store(true)
		close(rp.closeCh)
	})
	<-rp.doneCh
}

// 收到数据时（生产者生产数据）
func (rp *ReadProcessor) OnRecvBytes(data []byte) error {
	rp.mu.Lock()
	rp.recvBuf = append(rp.recvBuf, data...)
	// 粘包拆包
	packets, err := kkpacket.DefaultStreamPacket().Split(rp.recvBuf, nil)
	if err != nil {
		rp.mu.Unlock()
		return err
	}
	rp.recvBuf = rp.recvBuf[:0]

	wasEmpty := rp.recvQueue.IsEmpty()
	for _, packet := range packets {
		ok := rp.recvQueue.Push(packet)
		if !ok {
			// 丢弃并回收，避免泄漏
			kkbuffer.Put(packet)
		}
	}
	rp.mu.Unlock()

	// 唤醒消费携程，消费recvQueue中的数据。
	if wasEmpty {
		rp.wakeConsumer()
	}
	return nil
}

// 唤醒消费携程。
func (rp *ReadProcessor) wakeConsumer() {
	select {
	case rp.wakeCh <- struct{}{}:
	default:
	}
}

// 消费携程：消费recvQueue中的数据，并分发消息。
func (rp *ReadProcessor) consumeRecvQueue() {
	defer close(rp.doneCh)

	for {
		select {
		case <-rp.wakeCh:
		case <-rp.closeCh:
			// drain remaining then exit
			rp.drainOnce()
			return
		}
		rp.drainOnce()
	}
}

func (rp *ReadProcessor) drainOnce() {
	for {
		rp.mu.Lock()
		n := rp.recvQueue.PopMany(len(rp.batchBuf), rp.batchBuf, 0)
		rp.mu.Unlock()
		if n <= 0 {
			return
		}
		for i := 0; i < n; i++ {
			packet := rp.batchBuf[i]
			rp.batchBuf[i] = nil
			if packet == nil {
				continue
			}
			if rp.opts.NeedDecode {
				msg, msgID, err := kkpacket.DecodeStream(packet.B, kkpacket.DefaultStreamPacket())
				kkbuffer.Put(packet)
				if err != nil {
					continue
				}
				rp.dispatchMessage(msg, msgID)
			} else {
				rp.dispatchRaw(packet)
			}
		}
	}
}

// 分发消息到业务逻辑层
func (rp *ReadProcessor) dispatchMessage(msg any, msgID kkpacket.MSGID) {
	// call handler.OnMessage
	if rp.opts.MsgHandler != nil {
		rp.opts.MsgHandler(rp.connID, msg, msgID)
	}
}

// 分发原始数据到业务逻辑层
func (rp *ReadProcessor) dispatchRaw(data buffers.IBuffer) {
	// call handler.OnRaw
	if rp.opts.RawHandler != nil {
		rp.opts.RawHandler(rp.connID, data)
	}
}
