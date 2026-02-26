// 共享 WriteProcessor：多个连接共用同一个 WP
package kkprocessor

import (
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// SharedWriteProcessor 多个连接共用同一发送队列与 writeLoop
type SharedWriteProcessor struct {
	opts kknet.WriteOptions

	queue     *connBufRing
	queueMu   sync.Mutex
	queueCond *sync.Cond
	closing   atomic.Bool
	closeOnce sync.Once
	closeCh   chan struct{}
	doneCh    chan struct{}
	drainedCh chan struct{}
	wakeCh    chan struct{}

	connMu  sync.RWMutex
	connFns map[kknet.CONN_ID]kknet.WriteFunc

	batchBuf     []*kkbuffer.ByteBuffer
	batchConnBuf []connBuf
}

// NewSharedWriteProcessor 创建共享 WP
func NewSharedWriteProcessor(opts kknet.WriteOptions) *SharedWriteProcessor {
	size := opts.SendQueueSize
	if size <= 0 {
		size = 128
	}
	wp := &SharedWriteProcessor{
		opts:         opts,
		queue:        newConnBufRing(size, opts.SendQueueStrict),
		connFns:      make(map[kknet.CONN_ID]kknet.WriteFunc),
		batchBuf:     make([]*kkbuffer.ByteBuffer, opts.BatchWriteSize),
		batchConnBuf: make([]connBuf, opts.BatchWriteSize),
		closeCh:      make(chan struct{}),
		doneCh:       make(chan struct{}),
		drainedCh:    make(chan struct{}),
		wakeCh:       make(chan struct{}, 1),
	}
	wp.queueCond = sync.NewCond(&wp.queueMu)
	return wp
}

// RegisterConn 连接注册，提供其 writeFn
func (wp *SharedWriteProcessor) RegisterConn(connID kknet.CONN_ID, writeFn kknet.WriteFunc) {
	wp.connMu.Lock()
	wp.connFns[connID] = writeFn
	wp.connMu.Unlock()
}

// UnregisterConn 连接注销；会 Broadcast 以唤醒 Block 模式下等待空位的 SendBufferForConn
// 锁顺序：queueMu -> connMu，避免与 SendBufferForConn Block 循环死锁
func (wp *SharedWriteProcessor) UnregisterConn(connID kknet.CONN_ID) {
	wp.queueMu.Lock()
	wp.connMu.Lock()
	delete(wp.connFns, connID)
	wp.connMu.Unlock()
	wp.queueCond.Broadcast()
	wp.queueMu.Unlock()
}

// SendBufferForConn 为指定连接发送
func (wp *SharedWriteProcessor) SendBufferForConn(connID kknet.CONN_ID, buffer *kkbuffer.ByteBuffer) error {
	if wp.closing.Load() {
		kkbuffer.Put(buffer)
		return kkerrors.ErrConnectionClosed
	}
	wp.queueMu.Lock()
	if wp.queue.IsFull() {
		switch wp.opts.SendQueueFullAction {
		case kknet.EWpQueueFullActionBlock:
			for {
				if wp.closing.Load() {
					wp.queueMu.Unlock()
					kkbuffer.Put(buffer)
					return kkerrors.ErrConnectionClosed
				}
				wp.connMu.RLock()
				stillReg := wp.connFns[connID] != nil
				wp.connMu.RUnlock()
				if !stillReg {
					wp.queueMu.Unlock()
					kkbuffer.Put(buffer)
					return kkerrors.ErrConnectionClosed
				}
				if !wp.queue.IsFull() {
					wp.queue.Push(connBuf{connID: connID, buf: buffer})
					wp.queueCond.Broadcast()
					wp.queueMu.Unlock()
					wp.wakeWriter()
					return nil
				}
				wp.queueCond.Wait()
			}
		case kknet.EWpQueueFullActionDrop:
			wp.queueMu.Unlock()
			kkbuffer.Put(buffer)
			return nil
		case kknet.EWpQueueFullActionRetry:
			wp.queueMu.Unlock()
			interval := wp.opts.SendQueueRetryInterval
			if interval <= 0 {
				interval = 2 * time.Millisecond
			}
			for i := 0; i < wp.opts.SendQueueRetryMaxCount; i++ {
				time.Sleep(interval)
				wp.queueMu.Lock()
				if !wp.queue.IsFull() {
					wp.queue.Push(connBuf{connID: connID, buf: buffer})
					wp.queueCond.Broadcast()
					wp.queueMu.Unlock()
					wp.wakeWriter()
					return nil
				}
				wp.queueMu.Unlock()
			}
			kkbuffer.Put(buffer)
			return kkerrors.ErrSendQueueFull
		default:
			kkbuffer.Put(buffer)
			return nil
		}
	}
	wp.queue.Push(connBuf{connID: connID, buf: buffer})
	wp.queueCond.Broadcast()
	wp.queueMu.Unlock()
	wp.wakeWriter()
	return nil
}

func (wp *SharedWriteProcessor) wakeWriter() {
	select {
	case wp.wakeCh <- struct{}{}:
	default:
	}
}

// Start 启动 writeLoop
func (wp *SharedWriteProcessor) Start() {
	go wp.writeLoop()
}

// Stop 停止。若 SendQueueNeedFlushOver 且 err==nil，则等待队列 flush 完成或超时
func (wp *SharedWriteProcessor) Stop(err error) {
	wp.closeOnce.Do(func() {
		wp.closing.Store(true)
		wp.queueMu.Lock()
		wp.queueCond.Broadcast()
		wp.queueMu.Unlock()

		flush := wp.opts.SendQueueNeedFlushOver && err == nil
		if flush {
			wp.wakeWriter()
			timeout := wp.opts.SendQueueTimeoutFlushOver
			if timeout <= 0 {
				timeout = 10 * time.Second
			}
			select {
			case <-wp.drainedCh:
			case <-time.After(timeout):
				if wp.opts.SendQueueFlushTimeoutCallback != nil {
					wp.opts.SendQueueFlushTimeoutCallback(nil, timeout)
				}
				close(wp.closeCh)
			}
		} else {
			close(wp.closeCh)
		}
	})
	<-wp.doneCh
}

// Done 返回关闭完成的 channel
func (wp *SharedWriteProcessor) Done() <-chan struct{} {
	return wp.doneCh
}

// Pending 返回待发送队列中的消息数量
func (wp *SharedWriteProcessor) Pending() int {
	wp.queueMu.Lock()
	defer wp.queueMu.Unlock()
	if wp.queue == nil {
		return 0
	}
	return wp.queue.Len()
}

func (wp *SharedWriteProcessor) drainAndRelease() {
	wp.queueMu.Lock()
	wp.queue.DrainAndRelease(func(b *kkbuffer.ByteBuffer) { kkbuffer.Put(b) })
	wp.queueMu.Unlock()
}

func (wp *SharedWriteProcessor) writeLoop() {
	defer close(wp.doneCh)

	for {
		select {
		case <-wp.wakeCh:
		case <-wp.closeCh:
			wp.drainAndRelease()
			return
		}

		for {
			wp.queueMu.Lock()
			n := wp.queue.PopMany(wp.batchConnBuf, wp.opts.BatchWriteLimitBytes)
			remain := wp.queue.Len()
			wp.queueCond.Broadcast()
			wp.queueMu.Unlock()

			if n <= 0 {
				if wp.closing.Load() && remain == 0 {
					select {
					case <-wp.drainedCh:
					default:
						close(wp.drainedCh)
					}
					return
				}
				break
			}

			batch := wp.batchConnBuf[:n]
			j := 0
			for i := 0; i < n; i++ {
				cb := batch[i]
				wp.connMu.RLock()
				fn := wp.connFns[cb.connID]
				wp.connMu.RUnlock()
				if fn == nil {
					if cb.buf != nil {
						kkbuffer.Put(cb.buf)
					}
					continue
				}
				batch[j] = cb
				j++
			}
			batch = batch[:j]
			n = j

			if n == 0 {
				continue
			}

			i := 0
			for i < n {
				curConn := batch[i].connID
				wp.connMu.RLock()
				fn := wp.connFns[curConn]
				wp.connMu.RUnlock()
				if fn == nil {
					kkbuffer.Put(batch[i].buf)
					i++
					continue
				}
				j := i
				for j < n && batch[j].connID == curConn {
					wp.batchBuf[j-i] = batch[j].buf
					batch[j].buf = nil
					j++
				}
				count := j - i
				if err := fn(wp.batchBuf[:count], count); err != nil {
					if !wp.isWriteFnRetryable(err) {
						wp.drainReleaseBatch(wp.batchBuf[:count])
					} else if !wp.retryWriteFn(fn, curConn, count) {
						wp.drainReleaseBatch(wp.batchBuf[:count])
					}
				}
				for k := 0; k < count; k++ {
					wp.batchBuf[k] = nil
				}
				i = j
			}
		}
	}
}

func (wp *SharedWriteProcessor) isWriteFnRetryable(err error) bool {
	if wp.opts.WriteFnIsRetryable != nil {
		return wp.opts.WriteFnIsRetryable(err)
	}
	return sharedWpDefaultIsWriteFnRetryable(err)
}

func sharedWpDefaultIsWriteFnRetryable(err error) bool {
	if err == nil {
		return true
	}
	if errors.Is(err, kkerrors.ErrConnectionClosed) ||
		errors.Is(err, kkerrors.ErrInvalidPacket) ||
		errors.Is(err, kkerrors.ErrSendQueueFull) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, io.ErrClosedPipe) {
		return false
	}
	return true
}

func (wp *SharedWriteProcessor) drainReleaseBatch(batch []*kkbuffer.ByteBuffer) {
	for i := range batch {
		if batch[i] != nil {
			kkbuffer.Put(batch[i])
			batch[i] = nil
		}
	}
}

func (wp *SharedWriteProcessor) compactBatchBuf(batch []*kkbuffer.ByteBuffer, n int) int {
	j := 0
	for i := 0; i < n; i++ {
		if batch[i] != nil {
			if j != i {
				batch[j] = batch[i]
				batch[i] = nil
			}
			j++
		}
	}
	return j
}

// retryWriteFn 重试 writeFn，仅对 batch 中剩余未释放的 buffer 重试。
// writeFn 约定：成功发送的 buffer 由 writeFn 自行释放并置 nil，失败的保留在 batch 中。
func (wp *SharedWriteProcessor) retryWriteFn(fn kknet.WriteFunc, connID kknet.CONN_ID, n int) bool {
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

		wp.connMu.RLock()
		stillFn := wp.connFns[connID]
		wp.connMu.RUnlock()
		if stillFn == nil {
			return false
		}

		remaining := wp.compactBatchBuf(wp.batchBuf, n)
		if remaining <= 0 {
			return true
		}
		if err := fn(wp.batchBuf, remaining); err == nil {
			return true
		} else if !wp.isWriteFnRetryable(err) {
			return false
		}
	}
	return false
}
