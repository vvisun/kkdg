package kkprocessor

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/queues/bbqueue"
)

/**
 * 消息处理器-发送器。每个连接一个发送器。
 * 负责编码、然后将编码后的数据投入发送队列，供连接发送。
 * 使用 workerQueue(maxConcurrency=1) 串行执行写任务，替代原 writeLoop 协程。
 */
type WorkerWriteProcessor struct {
	conn   kknet.IConn   //连接(用于 flush 超时回调传参)
	connID kknet.CONN_ID //连接ID，记录下来，方便conn关闭导致conn为空时，消费携程可以继续消费。
	opts   kknet.WriteOptions

	sendQueue       bbqueue.IFiFoQueue                          //发送队列
	sendBatchBuffer [kknet.BatchPacketSize]*kkbuffer.ByteBuffer //批量发送缓冲区。as an array to reduce memory allocation.

	sendMu    sync.Mutex
	cond      *sync.Cond // 用于 Block 模式：队列有空位时由 drain 任务唤醒
	closeOnce sync.Once
	closing   atomic.Bool
	stopErr   error // Stop(err) 传入，供 shutdownJob 判断是否 flush

	wq *WorkerQueue // 写任务队列，maxConcurrency=1 保证顺序

	drainedCh chan struct{}
	doneCh    chan struct{}

	writeFn      kknet.WriteFunc
	onWriteError func(error)
}

var _ kknet.IWriteProcessor = (*WorkerWriteProcessor)(nil)

// 使用 workerQueue 调度写任务，不再单独起 writeLoop 协程。
func NewWorkerWriteProcessor(opts kknet.WriteOptions) kknet.IWriteProcessor {
	kknet.CheckWriteOptions(&opts)
	wp := &WorkerWriteProcessor{
		opts:      opts,
		sendQueue: bbqueue.NewFIFOQueue(opts.SendQueueSize, opts.SendQueueStrict),
		wq:        NewWorkerQueue(1), // 单 worker 串行写，保证顺序
		drainedCh: make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
	wp.cond = sync.NewCond(&wp.sendMu)
	return wp
}

func (wp *WorkerWriteProcessor) Done() <-chan struct{} { return wp.doneCh }

func (wp *WorkerWriteProcessor) Pending() int {
	wp.sendMu.Lock()
	defer wp.sendMu.Unlock()
	if wp.sendQueue == nil {
		return 0
	}
	return wp.sendQueue.Len()
}

// Start starts the write processor. writeFn must consume the buffers in batch
// (and clear wp.sendBatchBuffer[0:n] pointers) before returning.
// 使用 workerQueue 调度写任务，不再单独起 writeLoop 协程。
func (wp *WorkerWriteProcessor) Start(conn kknet.IConn, writeFn kknet.WriteFunc, onWriteError func(error)) {
	wp.conn = conn
	wp.writeFn = writeFn
	wp.onWriteError = onWriteError
}

func (wp *WorkerWriteProcessor) SendBuffer(buffer *kkbuffer.ByteBuffer) error {
	switch wp.opts.SendQueueFullAction {
	case kknet.EWpQueueFullActionDrop:
		return wp.sendBufferDrop(buffer)
	case kknet.EWpQueueFullActionBlock:
		return wp.sendBufferBlock(buffer)
	case kknet.EWpQueueFullActionRetry:
		return wp.sendBufferRetry(buffer)
	default:
		kkbuffer.Put(buffer)
		return nil
	}
}

// sendBufferDrop 队列满时丢弃，返回 nil
func (wp *WorkerWriteProcessor) sendBufferDrop(buffer *kkbuffer.ByteBuffer) error {
	wp.sendMu.Lock()
	if wp.closing.Load() {
		wp.sendMu.Unlock()
		kkbuffer.Put(buffer)
		return kkerrors.ErrConnectionClosed
	}
	wasEmpty := wp.sendQueue.IsEmpty()
	ok := wp.sendQueue.Push(buffer)
	nowEmpty := wp.sendQueue.IsEmpty()
	wp.sendMu.Unlock()

	if !ok {
		kkbuffer.Put(buffer)
		return nil
	}
	if wasEmpty && !nowEmpty {
		wp.wakeWriter()
	}
	return nil
}

// sendBufferBlock 队列满时阻塞等待，直到有空位或连接关闭
func (wp *WorkerWriteProcessor) sendBufferBlock(buffer *kkbuffer.ByteBuffer) error {
	wp.sendMu.Lock()
	defer wp.sendMu.Unlock()

	for {
		if wp.closing.Load() {
			kkbuffer.Put(buffer)
			return kkerrors.ErrConnectionClosed
		}
		wasEmpty := wp.sendQueue.IsEmpty()
		ok := wp.sendQueue.Push(buffer)
		if ok {
			if wasEmpty {
				wp.wakeWriter()
			}
			return nil
		}
		wp.cond.Wait()
	}
}

// sendBufferRetry 队列满时重试，带间隔退避，直到成功或达到最大重试次数
func (wp *WorkerWriteProcessor) sendBufferRetry(buffer *kkbuffer.ByteBuffer) error {
	interval := wp.opts.SendQueueRetryInterval
	if interval <= 0 {
		interval = 2 * time.Millisecond
	}
	maxCount := wp.opts.SendQueueRetryMaxCount

	for i := 0; ; i++ {
		wp.sendMu.Lock()
		if wp.closing.Load() {
			wp.sendMu.Unlock()
			kkbuffer.Put(buffer)
			return kkerrors.ErrConnectionClosed
		}
		wasEmpty := wp.sendQueue.IsEmpty()
		ok := wp.sendQueue.Push(buffer)
		if ok {
			wp.sendMu.Unlock()
			if wasEmpty {
				wp.wakeWriter()
			}
			return nil
		}
		wp.sendMu.Unlock()

		if maxCount > 0 && i >= maxCount-1 {
			kkbuffer.Put(buffer)
			return kkerrors.ErrSendQueueFull
		}
		time.Sleep(interval)
	}
}

// 发送消息。
func (wp *WorkerWriteProcessor) SendMsg(msg any) error {
	// 编码消息
	buffer, err := kkpacket.EncodeStream(msg, kkpacket.DefaultStreamPacket(), wp.opts.MsgPacket)
	if err != nil {
		kkbuffer.Put(buffer)
		return err
	}
	return wp.SendBuffer(buffer)
}

// 唤醒写任务：向 workerQueue 投递一次 drain 任务（队列由空变非空时调用）。
func (wp *WorkerWriteProcessor) wakeWriter() {
	wp.wq.Push(func() { wp.drainJob() })
}

func (wp *WorkerWriteProcessor) Stop(err error) {
	wp.closeOnce.Do(func() {
		wp.closing.Store(true)
		wp.stopErr = err
		wp.sendMu.Lock()
		wp.cond.Broadcast()
		wp.sendMu.Unlock()
		flush := wp.opts.SendQueueNeedFlushOver && err == nil
		wp.wq.Push(func() { wp.shutdownJob() })
		if flush {
			timeout := wp.opts.SendQueueTimeoutFlushOver
			if timeout <= 0 {
				timeout = 10 * time.Second
			}
			select {
			case <-wp.drainedCh:
			case <-time.After(timeout):
				if wp.opts.SendQueueFlushTimeoutCallback != nil && wp.conn != nil {
					wp.opts.SendQueueFlushTimeoutCallback(wp.conn, timeout)
				}
			}
		}
		<-wp.doneCh
	})
}

func (wp *WorkerWriteProcessor) drainRelease(n int) {
	for i := 0; i < n; i++ {
		bb := wp.sendBatchBuffer[i]
		wp.sendBatchBuffer[i] = nil
		if bb != nil {
			kkbuffer.Put(bb)
		}
	}
}

// drainJob 由 workerQueue 执行：从 sendQueue 取一批，writeFn 发送；若仍有剩余则再次投递 drainJob。
func (wp *WorkerWriteProcessor) drainJob() {
	sbbLen := len(wp.sendBatchBuffer)
	wp.sendMu.Lock()
	n := wp.sendQueue.PopMany(sbbLen, wp.sendBatchBuffer[:], wp.opts.BatchWriteLimitBytes)
	remain := wp.sendQueue.Len()
	closing := wp.closing.Load()
	if n > 0 {
		wp.cond.Signal()
	}
	wp.sendMu.Unlock()

	if n <= 0 {
		return
	}

	if err := wp.writeFn(wp.sendBatchBuffer[:n], n); err != nil {
		if !wp.isWriteFnRetryable(err) {
			wp.drainRelease(n)
			if wp.onWriteError != nil {
				wp.onWriteError(err)
			}
			return
		}
		if !wp.retryWriteFn(n) {
			wp.drainRelease(n)
			if wp.onWriteError != nil {
				wp.onWriteError(err)
			}
			return
		}
	}
	if remain > 0 && !closing {
		wp.wq.Push(func() { wp.drainJob() })
	}
}

// shutdownJob 由 Stop 投递：关闭时清空队列（flush 则 writeFn，否则 drainRelease），然后关闭 drainedCh、doneCh。
func (wp *WorkerWriteProcessor) shutdownJob() {
	defer close(wp.doneCh)
	sbbLen := len(wp.sendBatchBuffer)
	flush := wp.opts.SendQueueNeedFlushOver && wp.stopErr == nil
	for {
		wp.sendMu.Lock()
		n := wp.sendQueue.PopMany(sbbLen, wp.sendBatchBuffer[:], wp.opts.BatchWriteLimitBytes)
		wp.sendMu.Unlock()
		if n <= 0 {
			break
		}
		if flush {
			if err := wp.writeFn(wp.sendBatchBuffer[:n], n); err != nil {
				if !wp.isWriteFnRetryable(err) {
					wp.drainRelease(n)
					if wp.onWriteError != nil {
						wp.onWriteError(err)
					}
					flush = false
					break
				}
				if !wp.retryWriteFn(n) {
					wp.drainRelease(n)
					if wp.onWriteError != nil {
						wp.onWriteError(err)
					}
					flush = false
					break
				}
			}
		} else {
			wp.drainRelease(n)
		}
	}
	// 剩余未发送的一律 release
	for {
		wp.sendMu.Lock()
		n := wp.sendQueue.PopMany(sbbLen, wp.sendBatchBuffer[:], 0)
		wp.sendMu.Unlock()
		if n <= 0 {
			break
		}
		wp.drainRelease(n)
	}
	select {
	case <-wp.drainedCh:
	default:
		close(wp.drainedCh)
	}
}

// isWriteFnRetryable 判断 writeFn 的 error 是否可重试。不可重试则立即放弃。
func (wp *WorkerWriteProcessor) isWriteFnRetryable(err error) bool {
	if wp.opts.WriteFnIsRetryable != nil {
		return wp.opts.WriteFnIsRetryable(err)
	}
	return defaultIsWriteFnRetryable(err)
}

// retryWriteFn 重试 writeFn，仅对 batch 中剩余未释放的 buffer 重试。
// writeFn 约定：成功发送的 buffer 由 writeFn 自行释放并置 nil，失败的保留在 batch 中。
// 返回 true 表示重试成功（或无需重试），false 表示最终失败。
func (wp *WorkerWriteProcessor) retryWriteFn(n int) bool {
	maxRetry := wp.opts.WriteFnRetryMaxCount
	if maxRetry <= 0 {
		return false
	}
	interval := wp.opts.WriteFnRetryInterval
	if interval <= 0 {
		interval = 5 * time.Millisecond
	}

	for attempt := 1; attempt <= maxRetry; attempt++ {
		if wp.closing.Load() {
			return false
		}
		time.Sleep(interval)

		remaining := wp.compactBatch(n)
		if remaining <= 0 {
			return true
		}
		if err := wp.writeFn(wp.sendBatchBuffer[:remaining], remaining); err == nil {
			return true
		} else if !wp.isWriteFnRetryable(err) {
			return false
		}
	}
	return false
}

// compactBatch 将 batch[0:n] 中非 nil 的 buffer 紧凑到 batch 头部，返回剩余数量。
func (wp *WorkerWriteProcessor) compactBatch(n int) int {
	j := 0
	for i := 0; i < n; i++ {
		if wp.sendBatchBuffer[i] != nil {
			if j != i {
				wp.sendBatchBuffer[j] = wp.sendBatchBuffer[i]
				wp.sendBatchBuffer[i] = nil
			}
			j++
		}
	}
	return j
}
