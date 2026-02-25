// 共享 WriteProcessor：多个连接共用同一个 WP
package kkprocessor

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

type connBuf struct {
	connID kknet.CONN_ID
	buf    *kkbuffer.ByteBuffer
}

// SharedWriteProcessor 多个连接共用同一发送队列与 writeLoop
type SharedWriteProcessor struct {
	opts kknet.WriteOptions

	queue     []connBuf
	queueMu   sync.Mutex
	queueCond *sync.Cond
	closing   atomic.Bool
	closeOnce sync.Once
	closeCh   chan struct{}
	doneCh    chan struct{}
	wakeCh    chan struct{}

	connMu  sync.RWMutex
	connFns map[kknet.CONN_ID]kknet.WriteFunc

	batchBuf []*kkbuffer.ByteBuffer
}

// NewSharedWriteProcessor 创建共享 WP
func NewSharedWriteProcessor(opts kknet.WriteOptions) *SharedWriteProcessor {
	wp := &SharedWriteProcessor{
		opts:     opts,
		queue:    make([]connBuf, 0, opts.SendQueueSize*2),
		connFns:  make(map[kknet.CONN_ID]kknet.WriteFunc),
		batchBuf: make([]*kkbuffer.ByteBuffer, opts.BatchWriteSize),
		closeCh:  make(chan struct{}),
		doneCh:   make(chan struct{}),
		wakeCh:   make(chan struct{}, 1),
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

// UnregisterConn 连接注销
func (wp *SharedWriteProcessor) UnregisterConn(connID kknet.CONN_ID) {
	wp.connMu.Lock()
	delete(wp.connFns, connID)
	wp.connMu.Unlock()
}

// SendBufferForConn 为指定连接发送
func (wp *SharedWriteProcessor) SendBufferForConn(connID kknet.CONN_ID, buffer *kkbuffer.ByteBuffer) error {
	if wp.closing.Load() {
		kkbuffer.Put(buffer)
		return kkerrors.ErrConnectionClosed
	}
	wp.queueMu.Lock()
	if wp.opts.SendQueueStrict && len(wp.queue) >= wp.opts.SendQueueSize {
		wp.queueMu.Unlock()
		switch wp.opts.SendQueueFullAction {
		case kknet.EWpQueueFullActionDrop:
			kkbuffer.Put(buffer)
			return nil
		case kknet.EWpQueueFullActionRetry:
			interval := wp.opts.SendQueueRetryInterval
			if interval <= 0 {
				interval = 2 * time.Millisecond
			}
			for i := 0; i < wp.opts.SendQueueRetryMaxCount; i++ {
				time.Sleep(interval)
				wp.queueMu.Lock()
				if len(wp.queue) < wp.opts.SendQueueSize {
					wp.queue = append(wp.queue, connBuf{connID: connID, buf: buffer})
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
	wp.queue = append(wp.queue, connBuf{connID: connID, buf: buffer})
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

// Stop 停止
func (wp *SharedWriteProcessor) Stop(err error) {
	wp.closeOnce.Do(func() {
		wp.closing.Store(true)
		close(wp.closeCh)
		wp.queueMu.Lock()
		wp.queueCond.Broadcast()
		wp.queueMu.Unlock()
	})
	<-wp.doneCh
}

// Done 返回关闭完成的 channel
func (wp *SharedWriteProcessor) Done() <-chan struct{} {
	return wp.doneCh
}

func (wp *SharedWriteProcessor) drainAndRelease() {
	wp.queueMu.Lock()
	for _, cb := range wp.queue {
		if cb.buf != nil {
			kkbuffer.Put(cb.buf)
		}
	}
	wp.queue = wp.queue[:0]
	wp.queueMu.Unlock()
}

func (wp *SharedWriteProcessor) writeLoop() {
	defer close(wp.doneCh)

	sbbLen := len(wp.batchBuf)
	for {
		select {
		case <-wp.wakeCh:
		case <-wp.closeCh:
			wp.drainAndRelease()
			return
		}

		wp.queueMu.Lock()
		batch := make([]connBuf, 0, sbbLen)
		limitBytes := wp.opts.BatchWriteLimitBytes
		for len(wp.queue) > 0 && len(batch) < sbbLen {
			cb := wp.queue[0]
			wp.queue = wp.queue[1:]
			wp.connMu.RLock()
			fn := wp.connFns[cb.connID]
			wp.connMu.RUnlock()
			if fn == nil {
				if cb.buf != nil {
					kkbuffer.Put(cb.buf)
				}
				continue
			}
			batch = append(batch, cb)
			if limitBytes > 0 {
				total := 0
				for _, b := range batch {
					if b.buf != nil {
						total += b.buf.Len()
					}
				}
				if total >= limitBytes {
					break
				}
			}
		}
		wp.queueMu.Unlock()

		if len(batch) == 0 {
			if wp.closing.Load() {
				wp.queueMu.Lock()
				empty := len(wp.queue) == 0
				wp.queueMu.Unlock()
				if empty {
					return
				}
			}
			continue
		}

		// 按 connID 分组调用
		i := 0
		for i < len(batch) {
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
			for j < len(batch) && batch[j].connID == curConn {
				wp.batchBuf[j-i] = batch[j].buf
				batch[j].buf = nil
				j++
			}
			_ = fn(wp.batchBuf[:j-i], j-i)
			for k := 0; k < j-i; k++ {
				wp.batchBuf[k] = nil
			}
			i = j
		}
	}
}
