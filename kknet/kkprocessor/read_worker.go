package kkprocessor

import (
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/byteslice"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/xcall"
)

// 消息处理器-接收器。
//
// 与 ReadProcessor 的区别：
//   - 不再为每个连接维护 recvQueue + 消费协程；
//   - 每个完整 [length,message] 解析后封装为 task，投递到 workerQueue 执行。
//   - workerQueue并发数设为1时，和ReadProcessor基本一致，区别只在ReadProcessor为每个连接一个固定携程，而WorkerReadProcessor临时启动一个携程。
//   - workerQueue并发数大于1时，和ReadProcessor区别较大，WorkerReadProcessor不再保证顺序性。
//
// 主动关闭Server或Client后，只消费，不再接受数据入队。
type WorkerReadProcessor struct {
	conn   kknet.IConn
	connID kknet.CONN_ID
	opts   kknet.ReadOptions

	recvBuf  []byte                        // 残包缓冲区
	splitBuf [kknet.BatchPacketSize][]byte // 拆包缓冲区

	mu      sync.Mutex
	closing atomic.Bool

	workers *WorkerQueue
}

var _ kknet.IReadProcessor = (*WorkerReadProcessor)(nil)

// NewWorkerReadProcessor 创建基于 workerQueue 的 ReadProcessor。RawHandler必须设置，NoneCopyHandler会忽略。
// maxConcurrency 取自 ReadOptions.WorkerQueueMaxConcurrency（经 CheckReadOptions 归一化后范围在 [1,64]）。
// 默认 1，表示不并发，保证顺序性。大于1时并发，不保证顺序性。
func NewWorkerReadProcessor(opts kknet.ReadOptions) kknet.IReadProcessor {
	kknet.CheckReadOptions(&opts)
	if opts.RawHandler == nil {
		kklog.PanicLog("RawHandler is required for WorkerReadProcessor")
	}

	// 不保证RawHandler的顺序性，如果需要保证顺序，可以将RecvBatchSize设置为1。
	maxConc := opts.WorkerQueueMaxConcurrency
	if maxConc <= 0 {
		maxConc = 1
	}
	return &WorkerReadProcessor{
		opts:    opts,
		workers: NewWorkerQueue(maxConc),
	}
}

// Start 记录连接信息
func (rp *WorkerReadProcessor) Start(conn kknet.IConn) {
	rp.conn = conn
	if conn == nil {
		return
	}
	rp.connID = conn.ID()
}

// Stop 标记关闭；已入队但未执行的任务在执行时会检测 closing 标志并安全退出。
func (rp *WorkerReadProcessor) Stop() {
	rp.closing.Store(true)
}

// EnqueuePacket 适用于上层已完成切包的场景（如 gnet SplitSR 得到完整 [length,message]）。
func (rp *WorkerReadProcessor) EnqueuePacket(packet []byte) {
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
func (rp *WorkerReadProcessor) OnRecvBytes(data []byte) error {
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

	packets, leftData, err := rp.opts.StreamTool.Split(buf, rp.splitBuf[:0])
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
func (rp *WorkerReadProcessor) reRecvBuf(capacity int) {
	if rp.recvBuf == nil {
		rp.recvBuf = byteslice.GetZero(capacity)
		return
	}
	rp.recvBuf = rp.recvBuf[:0]
	byteslice.Put(rp.recvBuf)
	rp.recvBuf = byteslice.GetZero(capacity)
}

// submitTask 将一个完整包提交到 workerQueue，异步调用 RawHandler。
func (rp *WorkerReadProcessor) submitTask(bb *kkbuffer.ByteBuffer) {
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
