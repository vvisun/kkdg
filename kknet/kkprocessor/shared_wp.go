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

// connBufRing 环形队列，存 connBuf
type connBufRing struct {
	buf      []connBuf
	head     int
	tail     int
	count    int
	capacity int
	strict   bool
}

func newConnBufRing(size int, strict bool) *connBufRing {
	if size < 8 {
		size = 8
	}
	return &connBufRing{
		buf:      make([]connBuf, size),
		capacity: size,
		strict:   strict,
	}
}

func (q *connBufRing) Len() int   { return q.count }
func (q *connBufRing) Empty() bool { return q.count == 0 }

func (q *connBufRing) Full() bool {
	return q.strict && q.count >= q.capacity
}

func (q *connBufRing) Push(cb connBuf) bool {
	if q.strict && q.count >= q.capacity {
		return false
	}
	if q.count >= q.capacity {
		q.grow()
	}
	q.buf[q.tail] = cb
	q.tail = (q.tail + 1) % q.capacity
	q.count++
	return true
}

func (q *connBufRing) grow() {
	newCap := q.capacity * 2
	if newCap > 65536 {
		newCap = 65536
	}
	newBuf := make([]connBuf, newCap)
	for i := 0; i < q.count; i++ {
		pos := (q.head + i) % q.capacity
		newBuf[i] = q.buf[pos]
	}
	q.buf = newBuf
	q.head = 0
	q.tail = q.count
	q.capacity = newCap
}

func (q *connBufRing) Pop() (connBuf, bool) {
	if q.count == 0 {
		return connBuf{}, false
	}
	cb := q.buf[q.head]
	q.buf[q.head] = connBuf{}
	q.head = (q.head + 1) % q.capacity
	q.count--
	return cb, true
}

func (q *connBufRing) Peek() (connBuf, bool) {
	if q.count == 0 {
		return connBuf{}, false
	}
	return q.buf[q.head], true
}

func (q *connBufRing) PopMany(dst []connBuf, limitBytes int) int {
	if q.count == 0 || len(dst) == 0 {
		return 0
	}
	n := 0
	totalBytes := 0
	for n < len(dst) && q.count > 0 {
		pos := (q.head + n) % q.capacity
		cb := q.buf[pos]
		if limitBytes > 0 && n > 0 {
			if cb.buf != nil && totalBytes+cb.buf.Len() > limitBytes {
				break
			}
		}
		dst[n] = cb
		q.buf[pos] = connBuf{}
		n++
		if cb.buf != nil {
			totalBytes += cb.buf.Len()
		}
	}
	q.head = (q.head + n) % q.capacity
	q.count -= n
	return n
}

func (q *connBufRing) DrainAndRelease(release func(*kkbuffer.ByteBuffer)) {
	for q.count > 0 {
		cb, _ := q.Pop()
		if cb.buf != nil && release != nil {
			release(cb.buf)
		}
	}
}

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
	wakeCh    chan struct{}

	connMu  sync.RWMutex
	connFns map[kknet.CONN_ID]kknet.WriteFunc

	batchBuf   []*kkbuffer.ByteBuffer
	batchConnBuf []connBuf
}

// NewSharedWriteProcessor 创建共享 WP
func NewSharedWriteProcessor(opts kknet.WriteOptions) *SharedWriteProcessor {
	size := opts.SendQueueSize
	if size <= 0 {
		size = 128
	}
	wp := &SharedWriteProcessor{
		opts:        opts,
		queue:       newConnBufRing(size, opts.SendQueueStrict),
		connFns:     make(map[kknet.CONN_ID]kknet.WriteFunc),
		batchBuf:    make([]*kkbuffer.ByteBuffer, opts.BatchWriteSize),
		batchConnBuf: make([]connBuf, opts.BatchWriteSize),
		closeCh:     make(chan struct{}),
		doneCh:      make(chan struct{}),
		wakeCh:      make(chan struct{}, 1),
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
	if wp.queue.Full() {
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
				if !wp.queue.Full() {
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
				if !wp.queue.Full() {
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

		wp.queueMu.Lock()
		n := wp.queue.PopMany(wp.batchConnBuf, wp.opts.BatchWriteLimitBytes)
		wp.queueCond.Broadcast() // 唤醒 Block 模式下等待空位的 SendBufferForConn
		wp.queueMu.Unlock()

		if n <= 0 {
			if wp.closing.Load() {
				wp.queueMu.Lock()
				empty := wp.queue.Empty()
				wp.queueMu.Unlock()
				if empty {
					return
				}
			}
			continue
		}

		batch := wp.batchConnBuf[:n]
		// 过滤已注销连接，释放其 buffer
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

		// 按 connID 分组调用
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
			_ = fn(wp.batchBuf[:j-i], j-i)
			for k := 0; k < j-i; k++ {
				wp.batchBuf[k] = nil
			}
			i = j
		}
	}
}
