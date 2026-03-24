package kkprocessor

import (
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/queues/bbqueue"
	"github.com/vvisun/kkdg/utils/xcall"
)

// 消息处理器-接收器。单生产者。每个连接一个消费携程，采用的唤醒机制。
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

	packetSpliter PacketSpliter

	recvQueue bbqueue.IFiFoQueue //接收队列

	mu        sync.Mutex
	doneOnce  sync.Once
	closeOnce sync.Once
	closing   atomic.Bool
	started   atomic.Bool
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
		packetSpliter: NewPacketSpliter(opts.StreamTool, opts.RecvBufShrinkCap),
		recvQueue:     bbqueue.NewFIFOQueue(opts.RecvQueueSize, opts.RecvQueueStrict),
		opts:          opts,
		wakeCh:        make(chan struct{}, 1),
		closeCh:       make(chan struct{}),
		doneCh:        make(chan struct{}),
	}
}

func (rp *ReadProcessor) Pending() int {
	rp.mu.Lock()
	pending := rp.recvQueue.Len()
	rp.mu.Unlock()
	return pending
}

func (rp *ReadProcessor) Done() <-chan struct{} { return rp.doneCh }

// 连接建立时 / 启动消费协程
func (rp *ReadProcessor) Start(conn kknet.IConn) {
	rp.conn = conn
	if conn == nil {
		return
	}
	rp.connID = conn.ID()
	rp.started.Store(true)
	go rp.consumeRecvQueue()
}

// Stop 标记关闭并等待消费协程退出。
// Stop 返回后，不再接受新任务；队列中已接收的数据会继续消费完。
func (rp *ReadProcessor) Stop() {
	rp.closeOnce.Do(func() {
		rp.closing.Store(true)
		close(rp.closeCh)
		if !rp.started.Load() {
			rp.closeDone()
		}
	})
	<-rp.doneCh
}

func (rp *ReadProcessor) closeDone() {
	rp.doneOnce.Do(func() {
		close(rp.doneCh)
	})
}

// 收到单个完整包数据时（生产者生产数据）。
// 仅由网络读协程访问（单生产者），无需加锁；mu 仅保护 recvQueue。
func (rp *ReadProcessor) EnqueuePacket(packet []byte) error {
	if len(packet) == 0 {
		return nil
	}
	if rp.closing.Load() {
		return nil //关闭后，不再接受新任务。只消费已接收的数据。
	}

	bb := kkbuffer.GetWithCapacity(len(packet))
	bb.B = bb.B[:len(packet)]
	copy(bb.B, packet)

	rp.mu.Lock()
	if rp.closing.Load() {
		rp.mu.Unlock()
		kkbuffer.Put(bb)
		return nil //关闭后，不再接受新任务。只消费已接收的数据。
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

// 收到数据时（生产者生产数据）。
// recvBuf/splitBuf 仅由网络读协程访问（单生产者），无需加锁；mu 仅保护 recvQueue。
func (rp *ReadProcessor) OnRecvBytes(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	if rp.closing.Load() {
		return nil //关闭后，不再接受新任务。只消费已接收的数据。
	}

	// --- Phase 1: 拆包（单生产者，无锁） ---

	packets, err := rp.packetSpliter.Split(data)
	if err != nil {
		return err
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
		return nil //关闭后，不再接受新任务。只消费已接收的数据。
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
	defer rp.closeDone()

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

// dispatch message. 消费一次recvQueue中的数据，并分发给RawHandler。
func (rp *ReadProcessor) drainOnce() {
	connID := rp.connID
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
			rp.opts.RawHandler.OnRaw(connID, packet)
		}
	}
}
