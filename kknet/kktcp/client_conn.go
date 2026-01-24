package kktcp

import (
	"context"
	"io"
	"net"
	"sync"
	"time"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"golang.org/x/sync/semaphore"
)

const writeBatchSize = 16 // 每轮持锁时最多 Pop 的帧数，减少 Lock 次数与 Send 竞争

type clientConn struct {
	id    kknet.CONN_ID
	conn  net.Conn
	opts  kknet.Options
	stats *kknet.Stats

	sendMu      sync.Mutex
	sendData    *sync.Cond
	sendQueue   sendQueue
	batchBuffer [writeBatchSize]*kkbuffer.ByteBuffer
	sendLimit   int
	sendClosed  bool
	sendDrain   bool
	spaceSem    *semaphore.Weighted
	spaceCtx    context.Context
	spaceCancel context.CancelFunc
	spaceStrict bool //是否严格容量控制
	closeOnce   sync.Once

	ctxMu sync.RWMutex
	ctx   context.Context
}

var _ kknet.IConn = (*clientConn)(nil)

func newClientConn(conn net.Conn, opts kknet.Options, stats *kknet.Stats) *clientConn {
	queueLimit := opts.WriteBufferSize
	if queueLimit < kkpacket.DefaultMaxMessageSize() {
		queueLimit = kkpacket.DefaultMaxMessageSize()
	}
	spaceCtx, spaceCancel := context.WithCancel(context.Background())
	queueSize := opts.TcpClientSendQueueSize
	if queueSize <= 0 {
		queueSize = 64
	}
	cc := &clientConn{
		id:          kknet.NextConnID(),
		conn:        conn,
		opts:        opts,
		stats:       stats,
		sendLimit:   queueLimit,
		sendQueue:   newSendQueue(queueSize),
		spaceSem:    semaphore.NewWeighted(int64(queueLimit)),
		spaceCtx:    spaceCtx,
		spaceCancel: spaceCancel,
		ctx:         context.Background(),
	}
	cc.sendData = sync.NewCond(&cc.sendMu)
	return cc
}

func (c *clientConn) ID() kknet.CONN_ID {
	return c.id
}

func (c *clientConn) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}

func (c *clientConn) SendBuffer(bb buffers.IBuffer) error {
	if err := kkpacket.DefaultStreamPacket().CheckPacket(bb.B); err != nil {
		if c.stats != nil {
			c.stats.AddError()
		}
		return err
	}
	if len(bb.B) > c.sendLimit {
		if c.stats != nil {
			c.stats.AddError()
		}
		return kkerrors.ErrMaxMessageSize
	}

	// as bb is not used in other places, we can just use it directly
	// but maybe other places did Put(bb), so we need to get a new one
	// just swap, no need to copy. avoid other put(bb) and reuse, make bb dirty.
	oldBB := bb
	bb = kkbuffer.GetWithCapacity(len(oldBB.B))
	bb.B, oldBB.B = oldBB.B, bb.B
	kkbuffer.Put(oldBB)

	if err := c.spaceSem.Acquire(c.spaceCtx, int64(len(bb.B))); err != nil {
		kkbuffer.Put(bb)
		if c.stats != nil {
			c.stats.AddError()
		}
		return kkerrors.ErrConnectionClosed
	}

	c.sendMu.Lock()
	if c.sendClosed {
		c.sendMu.Unlock()
		c.spaceSem.Release(int64(len(bb.B)))
		kkbuffer.Put(bb)
		if c.stats != nil {
			c.stats.AddError()
		}
		return kkerrors.ErrConnectionClosed
	}
	wasEmpty := c.sendQueue.Len() == 0
	c.sendQueue.Push(bb)
	if wasEmpty {
		c.sendData.Signal()
	}
	c.sendMu.Unlock()
	return nil
}

func (c *clientConn) Send(data []byte) error {
	if err := kkpacket.DefaultStreamPacket().CheckPacket(data); err != nil {
		if c.stats != nil {
			c.stats.AddError()
		}
		return err
	}
	bb := kkbuffer.GetWithCapacity(len(data))
	bb.B = bb.B[:len(data)]
	copy(bb.B, data)
	return c.SendBuffer(bb)
}

func (c *clientConn) Close() error {
	var drained []*kkbuffer.ByteBuffer
	var drainedBytes int64
	c.sendMu.Lock()
	c.sendClosed = true
	c.sendDrain = c.opts.TcpClientNeedFlushOver
	flushTimeout := c.opts.TcpTimeoutFlushOver
	flushCb := c.opts.TcpClientFlushTimeoutCallback
	if !c.sendDrain {
		drained, drainedBytes = c.drainSendQueueLocked()
	}
	c.sendData.Broadcast()
	c.sendMu.Unlock()
	if drainedBytes > 0 {
		c.spaceSem.Release(drainedBytes)
		for _, bb := range drained {
			kkbuffer.Put(bb)
		}
	}
	c.spaceCancel()
	if !c.sendDrain {
		return c.conn.Close()
	}
	if flushTimeout > 0 {
		go func() {
			timer := time.NewTimer(flushTimeout)
			defer timer.Stop()
			<-timer.C
			shouldCallback := false
			var drained []*kkbuffer.ByteBuffer
			var drainedBytes int64
			c.sendMu.Lock()
			if c.sendClosed && c.sendDrain {
				shouldCallback = flushCb != nil
				c.sendDrain = false
				drained, drainedBytes = c.drainSendQueueLocked()
				c.sendData.Broadcast()
				if c.stats != nil {
					c.stats.AddError()
				}
				c.sendMu.Unlock()
				if drainedBytes > 0 {
					c.spaceSem.Release(drainedBytes)
					for _, bb := range drained {
						kkbuffer.Put(bb)
					}
				}
				if shouldCallback {
					flushCb(c, flushTimeout)
				}
				_ = c.conn.Close()
				if c.opts.Logger != nil {
					c.opts.Logger.Warnf("tcp client flush timeout: %v", flushTimeout)
				}
				return
			}
			c.sendMu.Unlock()
		}()
	}
	return nil
}

func (c *clientConn) Context() context.Context {
	c.ctxMu.RLock()
	defer c.ctxMu.RUnlock()
	return c.ctx
}

func (c *clientConn) SetContext(ctx context.Context) {
	c.ctxMu.Lock()
	c.ctx = ctx
	c.ctxMu.Unlock()
}

func (c *clientConn) readLoop(handler kknet.IHandler) error {
	header := make([]byte, kkpacket.DefaultStreamPacket().LengthFieldByteCount())
	for {
		if err := readFull(c.conn, header); err != nil {
			return err
		}
		size, err := kkpacket.DefaultStreamPacket().GetBodySize(header)
		if err != nil {
			return err
		}
		payload := kkbuffer.GetWithCapacity(size)
		payload.B = payload.B[:size]
		if err := readFull(c.conn, payload.B); err != nil {
			kkbuffer.Put(payload)
			return err
		}
		if c.stats != nil {
			c.stats.AddRecv(len(payload.B))
		}
		if handler != nil {
			kknet.SafeHandlerCall(c.opts.Logger, c.stats, "tcpclient OnMessage", func() {
				handler.OnMessage(c, payload)
			})
		}
		kkbuffer.Put(payload)
	}
}

func (c *clientConn) closeWithError(handler kknet.IHandler, err error) {
	c.closeOnce.Do(func() {
		var drained []*kkbuffer.ByteBuffer
		var drainedBytes int64
		c.sendMu.Lock()
		c.sendClosed = true
		c.sendDrain = false
		drained, drainedBytes = c.drainSendQueueLocked()
		c.sendData.Broadcast()
		c.sendMu.Unlock()
		if drainedBytes > 0 {
			c.spaceSem.Release(drainedBytes)
			for _, bb := range drained {
				kkbuffer.Put(bb)
			}
		}
		c.spaceCancel()
		if c.stats != nil {
			c.stats.OnClose()
			if err != nil {
				c.stats.AddError()
			}
		}
		_ = c.conn.Close()
		if handler != nil {
			kknet.SafeHandlerCall(c.opts.Logger, c.stats, "tcpclient OnClose", func() {
				handler.OnClose(c, err)
			})
		}
	})
}

func (c *clientConn) writeLoop() error {
	for {
		c.sendMu.Lock()
		for c.sendQueue.Len() == 0 && !c.sendClosed {
			c.sendData.Wait()
		}
		if c.sendClosed && !c.sendDrain {
			c.sendMu.Unlock()
			return nil
		}
		if c.sendQueue.Len() == 0 {
			if c.sendClosed && c.sendDrain {
				c.sendDrain = false
				c.sendMu.Unlock()
				_ = c.conn.Close()
				return nil
			}
			c.sendMu.Unlock()
			continue
		}
		// 批量 Pop，降低 Lock 竞争
		batch := c.batchBuffer[:0]
		for n := 0; n < writeBatchSize && c.sendQueue.Len() > 0; n++ {
			batch = append(batch, c.sendQueue.Pop())
		}
		c.sendMu.Unlock()

		for i := range batch {
			bb := batch[i]
			if !c.spaceStrict {
				c.spaceSem.Release(int64(len(bb.B)))
			}
			err := writeFull(c.conn, bb.B)
			kkbuffer.Put(bb)
			if err != nil {
				// 失败时：当前 bb 已 Put。spaceStrict 下当前 bb 尚未 Release，需补上；
				// batch 中尚未处理的需 Put+Release（已出队，drain 拿不到，否则 spaceSem 泄漏）
				if c.spaceStrict {
					c.spaceSem.Release(int64(len(bb.B)))
				}
				for _, b := range batch[i+1:] {
					kkbuffer.Put(b)
					c.spaceSem.Release(int64(len(b.B)))
				}
				if c.stats != nil {
					c.stats.AddError()
				}
				return err
			}
			if c.spaceStrict {
				c.spaceSem.Release(int64(len(bb.B)))
			}
			if c.stats != nil {
				c.stats.AddSent(len(bb.B))
			}
		}
	}
}

func (c *clientConn) drainSendQueueLocked() ([]*kkbuffer.ByteBuffer, int64) {
	if c.sendQueue.Len() == 0 {
		return nil, 0
	}
	drained := make([]*kkbuffer.ByteBuffer, 0, c.sendQueue.Len())
	var total int64
	for c.sendQueue.Len() > 0 {
		bb := c.sendQueue.Pop()
		if bb != nil {
			drained = append(drained, bb)
			total += int64(len(bb.B))
		}
	}
	return drained, total
}

type sendQueue struct {
	buf   []*kkbuffer.ByteBuffer
	head  int
	tail  int
	count int
}

func newSendQueue(size int) sendQueue {
	if size <= 0 {
		size = 64
	}
	return sendQueue{buf: make([]*kkbuffer.ByteBuffer, size)}
}

func (q *sendQueue) Len() int {
	return q.count
}

func (q *sendQueue) Push(bb *kkbuffer.ByteBuffer) {
	if q.count == len(q.buf) {
		q.grow()
	}
	q.buf[q.tail] = bb
	q.tail = (q.tail + 1) % len(q.buf)
	q.count++
}

func (q *sendQueue) Pop() *kkbuffer.ByteBuffer {
	if q.count == 0 {
		return nil
	}
	bb := q.buf[q.head]
	q.buf[q.head] = nil
	q.head = (q.head + 1) % len(q.buf)
	q.count--
	if q.count == 0 {
		q.head = 0
		q.tail = 0
	}
	return bb
}

func (q *sendQueue) grow() {
	newSize := len(q.buf) * 2
	if newSize == 0 {
		newSize = 64
	}
	newQueue := make([]*kkbuffer.ByteBuffer, newSize)
	if q.count > 0 {
		if q.head < q.tail {
			copy(newQueue, q.buf[q.head:q.tail])
		} else {
			n := copy(newQueue, q.buf[q.head:])
			copy(newQueue[n:], q.buf[:q.tail])
		}
	}
	q.buf = newQueue
	q.head = 0
	q.tail = q.count
}

func readFull(r io.Reader, buf []byte) error {
	_, err := io.ReadFull(r, buf)
	return err
}

func writeFull(w io.Writer, buf []byte) error {
	for len(buf) > 0 {
		n, err := w.Write(buf)
		if err != nil {
			return err
		}
		buf = buf[n:]
	}
	return nil
}
