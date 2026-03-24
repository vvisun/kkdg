package kkprocessor

import (
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/byteslice"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/queues/taskqueue"
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
//
// recvBuf/splitBuf 仅由网络读协程访问（单生产者），无需加锁；submitTask 内部线程安全。
type WorkerReadProcessor struct {
	conn   kknet.IConn
	connID kknet.CONN_ID
	opts   kknet.ReadOptions

	recvBuf  []byte                        // 残包缓冲区
	splitBuf [kknet.BatchPacketSize][]byte // 拆包缓冲区

	closing atomic.Bool
	wg      sync.WaitGroup
	pending atomic.Int64

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
	return int(rp.pending.Load())
}

// Start 记录连接信息
func (rp *WorkerReadProcessor) Start(conn kknet.IConn) {
	rp.conn = conn
	if conn == nil {
		return
	}
	rp.connID = conn.ID()
}

// Stop 标记关闭并等待所有已受理任务完成。
// Stop 返回后，不再接受新任务；已在 Stop 之前受理的任务会继续执行到 RawHandler 返回。
func (rp *WorkerReadProcessor) Stop() {
	rp.closing.Store(true)
	rp.wg.Wait()
}

func (rp *WorkerReadProcessor) tryAcquireRecvSlot() bool {
	if !rp.opts.RecvQueueStrict {
		rp.pending.Add(1)
		return true
	}
	limit := int64(rp.opts.RecvQueueSize)
	for {
		cur := rp.pending.Load()
		if cur >= limit {
			if cb := rp.opts.RecvQueueFullCallback; cb != nil {
				conn := rp.conn
				xcall.SafeCall(func() {
					cb(conn)
				})
			}
			return false
		}
		if rp.pending.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

func (rp *WorkerReadProcessor) releaseRecvSlot() {
	rp.pending.Add(-1)
}

// EnqueuePacket 适用于上层已完成切包的场景（如 gnet SplitSR 得到完整 [length,message]）。
func (rp *WorkerReadProcessor) EnqueuePacket(packet []byte) {
	if len(packet) == 0 {
		return
	}
	if rp.closing.Load() {
		return
	}

	if !rp.tryAcquireRecvSlot() {
		return
	}

	// 拷贝到池化 ByteBuffer，生命周期由 task 内部负责 Put。
	bb := kkbuffer.GetWithCapacity(len(packet))
	bb.B = bb.B[:len(packet)]
	copy(bb.B, packet)

	rp.submitTask(bb)
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
			return nil
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

	rp.wg.Add(1)

	if rp.closing.Load() {
		rp.wg.Done()
		rp.releaseRecvSlot()
		kkbuffer.Put(bb)
		return
	}

	connID := rp.connID
	rawHandler := rp.opts.RawHandler

	rp.workQueue.Push(func() {
		defer func() {
			rp.releaseRecvSlot()
			rp.wg.Done()
		}()
		xcall.SafeCallEx(func() {
			rawHandler.OnRaw(connID, bb)
		}, func(err any) {
			kkbuffer.Put(bb)
		})
	})
}
