package kkprocessor

import (
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/byteslice"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/queues/bbqueue"
	"github.com/vvisun/kkdg/utils/xcall"
)

const defaultRecvBufSize int = 1 * 1024 // 接收缓冲区大小，1KB

// 消息处理器-接收器。每个连接一个接收器。
//
//	启用独立携程消费recvQueue中的数据，并分发消息。
//	保证顺序性，适合RawHandler逻辑较重的场景。
//	RawHandler必须设置，NoneCopyHandler会忽略。
//
// 主动关闭Server或Client后，只消费，不再接受数据入队。
type ReadProcessor struct {
	conn   kknet.IConn       //连接
	connID kknet.CONN_ID     //连接ID，记录下来，方便conn关闭导致conn为空时，消费携程可以继续消费。
	opts   kknet.ReadOptions //选项

	recvBuf  []byte                        //残包缓冲区。初始化为nil，避免永远没残包还一直占内存。有残包再分配即可。
	splitBuf [kknet.BatchPacketSize][]byte //拆分缓冲区，用于拆分数据包时复用，避免分配新的内存

	recvQueue bbqueue.IFiFoQueue //接收队列

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
	if opts.RawHandler == nil {
		kklog.PanicLog("RawHandler is required")
	}

	return &ReadProcessor{
		recvBuf:   nil,
		recvQueue: bbqueue.NewFIFOQueue(opts.RecvQueueSize, opts.RecvQueueStrict),
		opts:      opts,
		wakeCh:    make(chan struct{}, 1),
		closeCh:   make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
}

func (rp *ReadProcessor) Pending() int {
	rp.mu.Lock()
	cnt := rp.recvQueue.Len()
	rp.mu.Unlock()
	return cnt
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

// EnqueuePacket enqueues a single, already-split packet frame: [length,message].
//
// This is useful for transports (like gnet) that already perform stream framing
// and can provide complete frames, avoiding a second Split/parse in OnRecvBytes.
//
// Note: packet bytes are copied into a pooled buffer because the input slice may
// reference ephemeral inbound buffers.
func (rp *ReadProcessor) EnqueuePacket(packet []byte) error {
	if len(packet) == 0 {
		return nil
	}
	if rp.closing.Load() {
		return nil
	}

	bb := kkbuffer.GetWithCapacity(len(packet))
	bb.B = bb.B[:len(packet)]
	copy(bb.B, packet)

	rp.mu.Lock()
	if rp.closing.Load() {
		rp.mu.Unlock()
		kkbuffer.Put(bb)
		return nil
	}
	wasEmpty := rp.recvQueue.IsEmpty()
	ok := rp.recvQueue.Push(bb)
	if !ok {
		kkbuffer.Put(bb)
		if cb := rp.opts.RecvQueueFullCallback; cb != nil {
			conn := rp.conn
			xcall.SafeCall(func() { cb(conn) })
		}
		nowEmpty := rp.recvQueue.IsEmpty()
		rp.mu.Unlock()
		if wasEmpty && !nowEmpty {
			rp.wakeConsumer()
		}
		return kkerrors.ErrNetRecvQueueFull
	}
	nowEmpty := rp.recvQueue.IsEmpty()
	rp.mu.Unlock()

	if wasEmpty && !nowEmpty {
		rp.wakeConsumer()
	}
	return nil
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

// 收到数据时（生产者生产数据）。
// recvBuf/splitBuf 仅由网络读协程访问（单生产者），无需加锁；mu 仅保护 recvQueue。
func (rp *ReadProcessor) OnRecvBytes(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	if rp.closing.Load() {
		return nil
	}

	// --- Phase 1: 拆包（单生产者，无锁） ---

	buf := data
	if len(rp.recvBuf) > 0 {
		rp.recvBuf = append(rp.recvBuf, data...)
		buf = rp.recvBuf
	}

	stream := rp.opts.StreamTool
	packets, leftData, err := stream.Split(buf, rp.splitBuf[:0])
	if err != nil {
		if rp.recvBuf != nil {
			rp.recvBuf = rp.recvBuf[:0]
		}
		return err
	}

	if len(leftData) > 0 {
		leftLen := len(leftData)
		rp.reRecvBuf(defaultRecvBufSize + leftLen)
		rp.recvBuf = rp.recvBuf[:leftLen]
		copy(rp.recvBuf, leftData)
	} else if rp.recvBuf != nil {
		rp.recvBuf = rp.recvBuf[:0]
	}

	if len(rp.recvBuf) == 0 && cap(rp.recvBuf) > rp.opts.RecvBufShrinkCap {
		byteslice.Put(rp.recvBuf)
		rp.recvBuf = nil
	}

	n := len(packets)
	if n == 0 {
		return nil
	}

	// --- Phase 2: 分配 ByteBuffer 并拷贝（无锁） ---

	var stackBuf [kknet.BatchPacketSize]*kkbuffer.ByteBuffer
	var prepared []*kkbuffer.ByteBuffer
	if n <= kknet.BatchPacketSize {
		prepared = stackBuf[:n]
	} else {
		prepared = make([]*kkbuffer.ByteBuffer, n)
	}
	for i, packet := range packets {
		bb := kkbuffer.GetWithCapacity(len(packet))
		bb.B = bb.B[:len(packet)]
		copy(bb.B, packet)
		prepared[i] = bb
	}

	// --- Phase 3: 入队（短锁，仅队列操作） ---

	fullIdx := -1
	rp.mu.Lock()
	if rp.closing.Load() {
		rp.mu.Unlock()
		for i := 0; i < n; i++ {
			if prepared[i] != nil {
				kkbuffer.Put(prepared[i])
			}
		}
		return nil
	}
	wasEmpty := rp.recvQueue.IsEmpty()
	for i := 0; i < n; i++ {
		ok := rp.recvQueue.Push(prepared[i])
		if !ok {
			fullIdx = i
			kkbuffer.Put(prepared[i])
			prepared[i] = nil
			break
		}
		prepared[i] = nil
	}
	nowEmpty := rp.recvQueue.IsEmpty()
	rp.mu.Unlock()

	if fullIdx >= 0 {
		for i := fullIdx + 1; i < n; i++ {
			if prepared[i] != nil {
				kkbuffer.Put(prepared[i])
			}
		}
		if cb := rp.opts.RecvQueueFullCallback; cb != nil {
			conn := rp.conn
			xcall.SafeCall(func() { cb(conn) })
		}
	}

	if wasEmpty && !nowEmpty {
		rp.wakeConsumer()
	}
	if fullIdx >= 0 {
		return kkerrors.ErrNetRecvQueueFull
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

const batchBufSize = 32

func (rp *ReadProcessor) drainOnce() {
	for {
		batchArr := [batchBufSize]*kkbuffer.ByteBuffer{}
		batchBuf := batchArr[:]
		rp.mu.Lock()
		n := rp.recvQueue.PopMany(batchBufSize, batchBuf, 0)
		rp.mu.Unlock()
		if n <= 0 {
			return
		}
		for i := 0; i < n; i++ {
			packet := batchBuf[i]
			if packet == nil {
				continue
			}
			//这里不用safe call, 防止将业务层致命错误静默吞避
			rp.opts.RawHandler.OnRaw(rp.connID, packet)
		}
	}
}
