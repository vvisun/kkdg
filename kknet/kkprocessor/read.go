package kkprocessor

import (
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/byteslice"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/queues/bbqueue"
	"github.com/vvisun/kkdg/utils/xcall"
)

const defaultRecvBufSize int = 1 * 1024 // 接收缓冲区大小，1KB

/**
 * 消息处理器-接收器。每个连接一个接收器。
 * 负责从连接中读取数据，并将其放入接收队列中。
 * 接收队列中的数据可以被其他组件消费。
 */
type ReadProcessor struct {
	conn   kknet.IConn       //连接
	connID kknet.CONN_ID     //连接ID，记录下来，方便conn关闭导致conn为空时，消费携程可以继续消费。
	userID kknet.USER_ID     //用户ID，记录下来，方便业务逻辑层使用。记录conn绑定的用户ID。
	opts   kknet.ReadOptions //选项

	recvQueue bbqueue.IFiFoQueue     //接收队列
	recvBuf   []byte                 //残包缓冲区。初始化为nil，避免永远没残包还一直占内存。有残包再分配即可。
	splitBuf  [32][]byte             //拆分缓冲区，用于拆分数据包时复用，避免分配新的内存
	batchBuf  []*kkbuffer.ByteBuffer //批量消费缓冲区，用于消费时复用，避免分配新的内存

	mu        sync.Mutex
	closeOnce sync.Once
	closing   atomic.Bool
	wakeCh    chan struct{}
	closeCh   chan struct{}
	doneCh    chan struct{}
}

var _ kknet.IReadProcessor = (*ReadProcessor)(nil)

func NewReadProcessor(opts kknet.ReadOptions) kknet.IReadProcessor {
	kknet.CheckReadOptions(&opts)
	if opts.NoneCopyHandler != nil {
		kklog.Warnf("ReadProcessor with NoneCopyHandler, NoneCopyHandler will be ignored")
	}
	if opts.RawHandler == nil {
		panic("RawHandler is required")
	}

	return &ReadProcessor{
		recvBuf:   nil,
		recvQueue: bbqueue.NewFIFOQueue(opts.RecvQueueSize, opts.RecvQueueStrict),
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

func (rp *ReadProcessor) reRecvBuf(capacity int) {
	if rp.recvBuf == nil {
		rp.recvBuf = byteslice.GetZero(capacity)
		return
	}
	rp.recvBuf = rp.recvBuf[:0]
	byteslice.Put(rp.recvBuf)
	rp.recvBuf = byteslice.GetZero(capacity)
}

// EnqueuePacket enqueues a single, already-split packet frame: [length,message].
//
// This is useful for transports (like gnet) that already perform stream framing
// and can provide complete frames, avoiding a second Split/parse in OnRecvBytes.
//
// Note: packet bytes are copied into a pooled buffer because the input slice may
// reference ephemeral inbound buffers.
func (rp *ReadProcessor) EnqueuePacket(packet []byte) {
	if len(packet) == 0 {
		return
	}
	rp.mu.Lock()
	wasEmpty := rp.recvQueue.IsEmpty()

	bb := kkbuffer.GetWithCapacity(len(packet))
	bb.B = bb.B[:len(packet)]
	copy(bb.B, packet)
	ok := rp.recvQueue.Push(bb)
	if !ok {
		kkbuffer.Put(bb)
		// RecvQueue full：丢弃并回调通知（如统计、限流、踢连接等）
		// 提示：“服务器繁忙” 或 “客户端发送过于频繁”
		if cb := rp.opts.RecvQueueFullCallback; cb != nil {
			conn := rp.conn
			xcall.SafeCall(func() { cb(conn) })
		}
	}

	nowEmpty := rp.recvQueue.IsEmpty()

	rp.mu.Unlock()

	// 唤醒消费携程，消费recvQueue中的数据。
	if wasEmpty && !nowEmpty {
		rp.wakeConsumer()
	}
}

// 收到数据时（生产者生产数据）
func (rp *ReadProcessor) OnRecvBytes(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	rp.mu.Lock()

	wasEmpty := rp.recvQueue.IsEmpty()

	buf := data
	if len(rp.recvBuf) > 0 {
		// 有残包，则将数据拼接到残包后面
		rp.recvBuf = append(rp.recvBuf, data...)
		buf = rp.recvBuf
	}

	// Parse [length,message][length,message]...
	stream := kkpacket.DefaultStreamPacket()
	packets, leftData, err := stream.Split(buf, rp.splitBuf[:0])
	if err != nil {
		if rp.recvBuf != nil {
			rp.recvBuf = rp.recvBuf[:0]
		}
		rp.mu.Unlock()
		return err
	}

	// 存下残包，下次收到数据时拼接到后面。
	if len(leftData) > 0 {
		leftLen := len(leftData)
		rp.reRecvBuf(defaultRecvBufSize + leftLen)
		rp.recvBuf = rp.recvBuf[:leftLen]
		copy(rp.recvBuf, leftData)
	}

	for _, packet := range packets {
		bb := kkbuffer.GetWithCapacity(len(packet))
		bb.B = bb.B[:len(packet)]
		copy(bb.B, packet)
		ok := rp.recvQueue.Push(bb)
		if !ok {
			kkbuffer.Put(bb)
			// RecvQueue full：丢弃并回调通知（如统计、限流、踢连接等）
			// 提示：“服务器繁忙” 或 “客户端发送过于频繁”
			if cb := rp.opts.RecvQueueFullCallback; cb != nil {
				conn := rp.conn
				xcall.SafeCall(func() { cb(conn) })
			}
		}
	}

	nowEmpty := rp.recvQueue.IsEmpty()

	// shrink: if empty and cap too big, shrink to default.
	if len(rp.recvBuf) == 0 && cap(rp.recvBuf) > rp.opts.RecvBufShrinkCap {
		byteslice.Put(rp.recvBuf)
		rp.recvBuf = nil
	}

	rp.mu.Unlock()

	// 唤醒消费携程，消费recvQueue中的数据。
	if wasEmpty && !nowEmpty {
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
		xcall.SafeCall(func() {
			for i := 0; i < n; i++ {
				packet := rp.batchBuf[i]
				rp.batchBuf[i] = nil
				if packet == nil {
					continue
				}
				rp.opts.RawHandler.OnRaw(rp.connID, packet)
			}
		})
	}
}
