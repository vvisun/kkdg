package kkprocessor

import (
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/byteslice"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/xcall"
)

// TaskReadProcessor 使用全局 workerQueue 并发执行 RawHandler，适合 CPU 绑定型解码/业务处理。
// 与 ReadProcessor 的区别：
//   - 不再为每个连接维护 recvQueue + 消费协程；
//   - 每个完整 [length,message] 解析后封装为 task，投递到 workerQueue 执行。
//
// 仅支持 RawHandler；如果设置了 NoneCopyHandler，应使用 SyncReadProcessor。
type TaskReadProcessor struct {
	conn   kknet.IConn
	connID kknet.CONN_ID
	opts   kknet.ReadOptions

	recvBuf  []byte                        // 残包缓冲区
	splitBuf [kknet.BatchPacketSize][]byte // 拆包缓冲区

	mu      sync.Mutex
	closing atomic.Bool

	workers *WorkerQueue
}

var _ kknet.IReadProcessor = (*TaskReadProcessor)(nil)

// NewTaskReadProcessor 创建基于 workerQueue 的 ReadProcessor。RawHandler必须设置，NoneCopyHandler会忽略。
// maxConcurrency 取自 ReadOptions.WorkerQueueMaxConcurrency（经 CheckReadOptions 归一化后范围在 [1,64]）。
// 默认 1，表示不并发，保证顺序性。大于1时并发，不保证顺序性。
func NewTaskReadProcessor(opts kknet.ReadOptions) kknet.IReadProcessor {
	kknet.CheckReadOptions(&opts)
	if opts.RawHandler == nil {
		panic("RawHandler is required for TaskReadProcessor")
	}

	// 不保证RawHandler的顺序性，如果需要保证顺序，可以将RecvBatchSize设置为1。
	maxConc := opts.WorkerQueueMaxConcurrency
	if maxConc <= 0 {
		maxConc = 1
	}
	return &TaskReadProcessor{
		opts:    opts,
		workers: NewWorkerQueue(maxConc),
	}
}

// Start 记录连接信息；TaskReadProcessor 不需要单独消费协程。
func (rp *TaskReadProcessor) Start(conn kknet.IConn) {
	rp.conn = conn
	if conn == nil {
		return
	}
	rp.connID = conn.ID()
}

// Stop 标记关闭；已入队但未执行的任务在执行时会检测 closing 标志并安全退出。
func (rp *TaskReadProcessor) Stop() {
	rp.closing.Store(true)
}

// EnqueuePacket 适用于上层已完成切包的场景（如 gnet SplitSR 得到完整 [length,message]）。
func (rp *TaskReadProcessor) EnqueuePacket(packet []byte) {
	if len(packet) == 0 {
		return
	}

	// 拷贝到池化 ByteBuffer，生命周期由 task 内部负责 Put。
	bb := kkbuffer.GetWithCapacity(len(packet))
	bb.B = bb.B[:len(packet)]
	copy(bb.B, packet)

	rp.submitTask(bb)
}

// OnRecvBytes 负责从字节流中拆出 [length,message] 帧，并将每帧封装为 task 投递到 workerQueue。
func (rp *TaskReadProcessor) OnRecvBytes(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	rp.mu.Lock()
	defer rp.mu.Unlock()

	buf := data
	if len(rp.recvBuf) > 0 {
		// 有残包，则将数据拼接到残包后面
		rp.recvBuf = append(rp.recvBuf, data...)
		buf = rp.recvBuf
	}

	stream := kkpacket.DefaultStreamPacket()
	packets, leftData, err := stream.Split(buf, rp.splitBuf[:0])
	if err != nil {
		if rp.recvBuf != nil {
			rp.recvBuf = rp.recvBuf[:0]
		}
		return err
	}

	// 存下残包，下次收到数据时拼接到后面。
	if len(leftData) > 0 {
		leftLen := len(leftData)
		rp.reRecvBuf(defaultRecvBufSize + leftLen)
		rp.recvBuf = rp.recvBuf[:leftLen]
		copy(rp.recvBuf, leftData)
	}

	// shrink: if empty and cap too big, shrink to default.
	if len(rp.recvBuf) == 0 && cap(rp.recvBuf) > rp.opts.RecvBufShrinkCap {
		byteslice.Put(rp.recvBuf)
		rp.recvBuf = nil
	}

	// 将每个完整包封装为 task；拷贝在锁内完成，避免 data 生命周期问题。
	for _, packet := range packets {
		if len(packet) == 0 {
			continue
		}
		bb := kkbuffer.GetWithCapacity(len(packet))
		bb.B = bb.B[:len(packet)]
		copy(bb.B, packet)
		rp.submitTask(bb)
	}

	return nil
}

// reRecvBuf 重新分配残包缓冲区。
func (rp *TaskReadProcessor) reRecvBuf(capacity int) {
	if rp.recvBuf == nil {
		rp.recvBuf = byteslice.GetZero(capacity)
		return
	}
	rp.recvBuf = rp.recvBuf[:0]
	byteslice.Put(rp.recvBuf)
	rp.recvBuf = byteslice.GetZero(capacity)
}

// submitTask 将一个完整包提交到 workerQueue，异步调用 RawHandler。
func (rp *TaskReadProcessor) submitTask(bb *kkbuffer.ByteBuffer) {
	if bb == nil {
		return
	}

	// 如果已经关闭，直接回收 buffer。
	if rp.closing.Load() {
		kkbuffer.Put(bb)
		return
	}

	connID := rp.connID
	rawHandler := rp.opts.RawHandler

	rp.workers.Push(func() {
		// 多线程执行 RawHandler，需要防护 panic
		xcall.SafeCall(func() {
			// Stop 之后进来的任务在这里二次检查 closing，尽量减少无意义处理。
			if rp.closing.Load() {
				kkbuffer.Put(bb)
				return
			}
			rawHandler.OnRaw(connID, bb)
		})
	})
}
