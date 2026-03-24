package kkprocessor

import (
	"sync/atomic"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/byteslice"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/queues/taskqueue"
	"github.com/vvisun/kkdg/utils/xcall"
)

// 消息处理器-接收器。
//
// 本质上可视为 ReadProcessor 的 workerQueue 调度版：
//   - 两者都会把完整 [length,message] 包派发到 RawHandler.OnRaw；
//   - ReadProcessor 通过每连接 recvQueue + 唤醒固定消费协程派发；
//   - WorkerReadProcessor 将每个完整包封装为 task，投递到 workerQueue 派发。
//
// 调度差异：
//   - workerQueue 并发数为 1 时，行为上接近 ReadProcessor；
//   - workerQueue 并发数大于 1 时，不再保证顺序性。
//
// 主动关闭Server或Client后，只消费，不再接受数据入队。
//
// recvBuf/splitBuf 仅由网络读协程访问（单生产者），无需加锁；submitTask 内部线程安全。
type WorkerReadProcessor struct {
	conn   kknet.IConn
	connID kknet.CONN_ID
	opts   kknet.ReadOptions

	recvBuf  []byte                        // 残包缓冲区
	splitBuf [kknet.BatchPacketSize][]byte // 拆包缓冲区

	closing atomic.Bool

	workQueue *taskqueue.WorkerQueue
}

var _ kknet.IReadProcessor = (*WorkerReadProcessor)(nil)

func NewWorkerReadProcessor(opts kknet.ReadOptions) kknet.IReadProcessor {
	kknet.CheckReadOptions(&opts)
	if opts.RawHandler == nil {
		kklog.PanicLog("RawHandler is required for WorkerReadProcessor")
	}

	maxConc := opts.WorkerQueueMaxConcurrency
	if maxConc <= 0 {
		maxConc = 1
	}
	return &WorkerReadProcessor{
		opts:      opts,
		workQueue: taskqueue.NewWorkerQueue(maxConc),
	}
}

func (rp *WorkerReadProcessor) Pending() int {
	return rp.workQueue.Len()
}

// Start 记录连接信息
func (rp *WorkerReadProcessor) Start(conn kknet.IConn) {
	rp.conn = conn
	if conn == nil {
		return
	}
	rp.connID = conn.ID()
}

// Stop 仅标记关闭，不等待已受理任务完成。
// Stop 返回后，不再接受新任务；已在 Stop 之前投递到 workerQueue 的任务仍可能继续异步执行。
func (rp *WorkerReadProcessor) Stop() {
	rp.closing.Store(true)
}

func (rp *WorkerReadProcessor) tryAcquireRecvSlot() bool {
	if !rp.opts.RecvQueueStrict {
		return true
	}
	if rp.workQueue.Len() >= rp.opts.RecvQueueSize {
		if cb := rp.opts.RecvQueueFullCallback; cb != nil {
			conn := rp.conn
			xcall.SafeCall(func() { cb(conn) })
		}
		return false
	}
	return true
}

// 收到单个完整包数据时（生产者生产数据）。
// 仅由网络读协程访问（单生产者），无需加锁。
func (rp *WorkerReadProcessor) EnqueuePacket(packet []byte) error {
	if len(packet) == 0 {
		return nil
	}
	if rp.closing.Load() {
		return nil
	}

	if !rp.tryAcquireRecvSlot() {
		return kkerrors.ErrNetRecvQueueFull
	}

	// 拷贝到池化 ByteBuffer，生命周期由 task 内部负责 Put。
	bb := kkbuffer.GetWithCapacity(len(packet))
	bb.B = bb.B[:len(packet)]
	copy(bb.B, packet)

	rp.submitTask(bb)
	return nil
}

// OnRecvBytes 负责从字节流中拆出 [length,message] 帧，并将每帧封装为 task 投递到 workerQueue。
// recvBuf/splitBuf 仅由网络读协程访问（单生产者），无需加锁。
func (rp *WorkerReadProcessor) OnRecvBytes(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	buf := data
	if len(rp.recvBuf) > 0 {
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

	for _, packet := range packets {
		if len(packet) == 0 {
			continue
		}
		if !rp.tryAcquireRecvSlot() {
			return kkerrors.ErrNetRecvQueueFull
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

// dispatch message. 将一个完整包提交到 workerQueue，异步调用 RawHandler。
func (rp *WorkerReadProcessor) submitTask(bb *kkbuffer.ByteBuffer) {
	if bb == nil {
		return
	}

	if rp.closing.Load() {
		kkbuffer.Put(bb)
		return
	}

	connID := rp.connID
	rawHandler := rp.opts.RawHandler

	rp.workQueue.Push(func() {
		//这里不用safe call, 防止将业务层致命错误静默吞避
		rawHandler.OnRaw(connID, bb)
	})
}
