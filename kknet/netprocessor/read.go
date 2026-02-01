package netprocessor

import (
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/queues/bbqueue"
	"github.com/vvisun/kkdg/utils/xcall"
)

type ReadOptions struct {
	RecvQueueSize   int               //接收队列大小
	RecvQueueStrict bool              //接收队列是否严格容量控制
	MsgHandler      kknet.IMsgHandler //消费函数, msg: object, msgID: 消息ID
	RawHandler      kknet.IRawHandler //消费函数, data: [length,message], 外部自行用解码器解码（内置的解码器见kkpacket/message_parser.go）
	RecvBatchSize   int               //每轮消费最多 Pop 的帧数
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
	conn      kknet.IConn              //连接
	connID    kknet.CONN_ID            //连接ID，记录下来，方便conn关闭导致conn为空时，消费携程可以继续消费。
	userID    int64                    //用户ID，记录下来，方便业务逻辑层使用。记录conn绑定的用户ID。
	recvBuf   []byte                   //接收缓冲区
	recvQueue *bbqueue.BBQueue         //接收队列
	splitBuf  [64]*kkbuffer.ByteBuffer //拆分缓冲区
	opts      ReadOptions              //选项

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
	if len(data) == 0 {
		return nil
	}
	rp.mu.Lock()
	// Fast path: avoid copying `data` into recvBuf unless we have leftover bytes.
	buf := data
	if len(rp.recvBuf) > 0 {
		rp.recvBuf = append(rp.recvBuf, data...)
		buf = rp.recvBuf
	}
	bufIsRecv := len(rp.recvBuf) > 0 && len(buf) > 0 && &buf[0] == &rp.recvBuf[0]

	wasEmpty := rp.recvQueue.IsEmpty()

	// Parse [length,message][length,message]...
	stream := kkpacket.DefaultStreamPacket()
	lfb := stream.LengthFieldByteCount()
	pos := 0

	for {
		if len(buf)-pos < lfb {
			break
		}
		sz, err := stream.GetBodySize(buf[pos:])
		if err != nil {
			// drop buffered bytes to avoid being stuck on invalid header
			rp.recvBuf = rp.recvBuf[:0]
			rp.mu.Unlock()
			return err
		}
		total := lfb + sz
		if len(buf)-pos < total {
			break
		}

		pkt := kkbuffer.GetWithCapacity(total)
		pkt.B = pkt.B[:total]
		copy(pkt.B, buf[pos:pos+total])
		ok := rp.recvQueue.Push(pkt)
		if !ok {
			kkbuffer.Put(pkt)
		}
		pos += total
	}

	// Preserve leftover bytes (incomplete packet) for next call.
	if pos == len(buf) {
		if len(rp.recvBuf) > 0 {
			rp.recvBuf = rp.recvBuf[:0]
		}
	} else if pos > 0 {
		left := buf[pos:]
		if bufIsRecv {
			copy(rp.recvBuf, left)
			rp.recvBuf = rp.recvBuf[:len(left)]
		} else {
			if cap(rp.recvBuf) < len(left) {
				rp.recvBuf = make([]byte, 0, len(left))
			}
			rp.recvBuf = rp.recvBuf[:len(left)]
			copy(rp.recvBuf, left)
		}
	} else {
		// pos == 0: no complete packet. Ensure we buffer all bytes for next time.
		if !bufIsRecv {
			if cap(rp.recvBuf) < len(data) {
				rp.recvBuf = make([]byte, 0, len(data))
			}
			rp.recvBuf = rp.recvBuf[:len(data)]
			copy(rp.recvBuf, data)
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
			if rp.opts.MsgHandler != nil {
				msg, msgID, err := kkpacket.DecodeStream(packet.B, kkpacket.DefaultStreamPacket())
				kkbuffer.Put(packet)
				if err != nil {
					continue
				}
				rp.dispatchMessage(msg, msgID)
			} else if rp.opts.RawHandler != nil {
				rp.dispatchRaw(packet)
			} else {
				kkbuffer.Put(packet)
			}
		}
	}
}

// 分发消息到业务逻辑层
func (rp *ReadProcessor) dispatchMessage(msg any, msgID kkpacket.MSGID) {
	xcall.SafeCall(func() {
		rp.opts.MsgHandler.OnMsg(rp.connID, msg, msgID)
	})
}

// 分发原始数据到业务逻辑层
func (rp *ReadProcessor) dispatchRaw(data buffers.IBuffer) {
	xcall.SafeCall(func() {
		rp.opts.RawHandler.OnRaw(rp.connID, data)
	})
}
