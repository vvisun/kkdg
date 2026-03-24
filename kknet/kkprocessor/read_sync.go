package kkprocessor

import (
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/byteslice"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/xcall"
)

// 消息处理器-接收器。
//
//	同步消费数据，实现0拷贝优化。NoneCopyHandler必须设置，RawHandler会忽略。
//	保证顺序性，适合NoneCopyHandler逻辑非常轻的场景。
//
// 主动关闭Server或Client后，只消费，不再接受数据入队。
//
//	此外，也可以让外部自行选择，是否启动携程来消费数据。
//	注意，外部如果启动携程，需要自己拷贝数据，因为底层会覆盖数据，携程中的数据不再是原始数据。
type SyncReadProcessor struct {
	conn   kknet.IConn
	connID kknet.CONN_ID     //连接ID，记录下来，方便conn关闭导致conn为空时，消费携程可以继续消费。
	opts   kknet.ReadOptions //选项

	recvBuf  []byte                        //残包缓冲区。初始化为nil，避免永远没残包还一直占内存。有残包再分配即可。
	splitBuf [kknet.BatchPacketSize][]byte //拆分缓冲区，用于拆分数据包时复用，避免分配新的内存

	mu sync.Mutex // 仅保护 Handler 串行调用（与 EnqueuePacket 互斥）

	recvQueueSize int64 // 接收队列大小
}

var _ kknet.IReadProcessor = (*SyncReadProcessor)(nil)

func NewSyncReadProcessor(opts kknet.ReadOptions) kknet.IReadProcessor {
	kknet.CheckReadOptions(&opts)
	if opts.NoneCopyHandler == nil {
		kklog.PanicLog("NoneCopyHandler is required")
	}

	return &SyncReadProcessor{
		recvBuf: nil,
		opts:    opts,
	}
}

func (rp *SyncReadProcessor) Pending() int {
	return 0
}

func (rp *SyncReadProcessor) Start(conn kknet.IConn) {
	rp.conn = conn
	rp.connID = conn.ID()
}

func (rp *SyncReadProcessor) Stop() {
	rp.conn = nil
}

func (rp *SyncReadProcessor) tryAcquireRecvSlot() bool {
	if !rp.opts.RecvQueueStrict {
		atomic.AddInt64(&rp.recvQueueSize, 1)
		return true
	}
	limit := int64(rp.opts.RecvQueueSize)
	for {
		cur := atomic.LoadInt64(&rp.recvQueueSize)
		if cur >= limit {
			if cb := rp.opts.RecvQueueFullCallback; cb != nil {
				conn := rp.conn
				xcall.SafeCall(func() { cb(conn) })
			}
			return false
		}
		if atomic.CompareAndSwapInt64(&rp.recvQueueSize, cur, cur+1) {
			return true
		}
	}
}

func (rp *SyncReadProcessor) releaseRecvSlot() {
	atomic.AddInt64(&rp.recvQueueSize, -1)
}

func (rp *SyncReadProcessor) EnqueuePacket(packet []byte) error {
	if len(packet) == 0 {
		return nil
	}
	if !rp.tryAcquireRecvSlot() {
		return kkerrors.ErrNetRecvQueueFull
	}
	defer rp.releaseRecvSlot()

	rp.mu.Lock()
	//这里不用safe call, 防止将业务层致命错误静默吞避
	rp.opts.NoneCopyHandler.OnNoneCopy(rp.connID, packet)
	rp.mu.Unlock()
	return nil
}

func (rp *SyncReadProcessor) reRecvBuf(capacity int) {
	if rp.recvBuf == nil {
		rp.recvBuf = byteslice.GetZero(capacity)
		return
	}
	rp.recvBuf = rp.recvBuf[:0]
	byteslice.Put(rp.recvBuf)
	rp.recvBuf = byteslice.GetZero(capacity)
}

// recvBuf/splitBuf 仅由网络读协程访问（单生产者），无需加锁；mu 仅保护 Handler 串行调用。
func (rp *SyncReadProcessor) OnRecvBytes(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	// 拆包（单生产者，无锁）
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

	if len(packets) == 0 {
		return nil
	}

	if !rp.tryAcquireRecvSlot() {
		return nil
	}
	defer rp.releaseRecvSlot()

	// Handler 串行调用（与 EnqueuePacket 互斥）
	rp.mu.Lock()
	for _, packet := range packets {
		// 这里不用safe call, 防止将业务层致命错误静默吞避
		rp.opts.NoneCopyHandler.OnNoneCopy(rp.connID, packet)
	}
	rp.mu.Unlock()

	return nil
}
