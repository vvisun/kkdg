package kkprocessor

import (
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/kklog"
)

// 消息处理器-接收器。单生产者。同步消费数据，实现0拷贝优化。
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

	mu            sync.Mutex // 仅保护 Handler 串行调用（与 EnqueuePacket 互斥）
	closing       atomic.Bool
	packetSpliter PacketSpliter
}

var _ kknet.IReadProcessor = (*SyncReadProcessor)(nil)

func NewSyncReadProcessor(opts kknet.ReadOptions) kknet.IReadProcessor {
	kknet.CheckReadOptions(&opts)
	if opts.NoneCopyHandler == nil {
		kklog.PanicLog("NoneCopyHandler is required")
	}

	return &SyncReadProcessor{
		opts:          opts,
		packetSpliter: NewPacketSpliter(opts.StreamTool, opts.RecvBufShrinkCap),
	}
}

func (rp *SyncReadProcessor) Pending() int {
	return 0 // SyncReadProcessor 因无异步队列，所以固定为 0。
}

func (rp *SyncReadProcessor) Start(conn kknet.IConn) {
	rp.conn = conn
	if conn == nil {
		return
	}
	rp.closing.Store(false)
	rp.connID = conn.ID()
}

// Stop 标记关闭，并等待当前同步处理退出临界区。
// Stop 返回后，不再接受新任务；由于无异步等待队列，也不会再有额外待派发任务。
func (rp *SyncReadProcessor) Stop() {
	rp.closing.Store(true)
	rp.mu.Lock()
	rp.conn = nil
	rp.mu.Unlock()
}

func (rp *SyncReadProcessor) tryAcquireRecvSlot() bool {
	return true
}

// 收到单个完整包数据时（生产者生产数据）。
// 仅由网络读协程访问（单生产者），无需加锁。mu 仅保护 Handler 串行调用。
func (rp *SyncReadProcessor) EnqueuePacket(packet []byte) error {
	if len(packet) == 0 {
		return nil
	}
	if rp.closing.Load() {
		return nil //关闭后，不再接受新任务。只消费已接收的数据。
	}
	if !rp.tryAcquireRecvSlot() {
		return kkerrors.ErrNetRecvQueueFull
	}

	rp.mu.Lock()
	if rp.closing.Load() {
		rp.mu.Unlock()
		return nil //关闭后，不再接受新任务。只消费已接收的数据。
	}
	// dispatch message.
	//这里不用safe call, 防止将业务层致命错误静默吞避
	rp.opts.NoneCopyHandler.OnNoneCopy(rp.connID, packet)
	rp.mu.Unlock()
	return nil
}

// recvBuf/splitBuf 仅由网络读协程访问（单生产者），无需加锁；mu 仅保护 Handler 串行调用。
func (rp *SyncReadProcessor) OnRecvBytes(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	if rp.closing.Load() {
		return nil //关闭后，不再接受新任务。只消费已接收的数据。
	}

	// 拆包（单生产者，无锁）
	packets, err := rp.packetSpliter.Split(data)
	if err != nil {
		return err
	}

	if len(packets) == 0 {
		return nil
	}
	if rp.closing.Load() {
		return nil //关闭后，不再接受新任务。只消费已接收的数据。
	}

	if !rp.tryAcquireRecvSlot() {
		return kkerrors.ErrNetRecvQueueFull
	}

	// dispatch message.
	// Handler 串行调用（与 EnqueuePacket 互斥）
	rp.mu.Lock()
	if rp.closing.Load() {
		rp.mu.Unlock()
		return nil //关闭后，不再接受新任务。只消费已接收的数据。
	}
	for _, packet := range packets {
		// 这里不用safe call, 防止将业务层致命错误静默吞避
		rp.opts.NoneCopyHandler.OnNoneCopy(rp.connID, packet)
	}
	rp.mu.Unlock()

	return nil
}
