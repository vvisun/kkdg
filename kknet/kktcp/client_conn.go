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
	"github.com/vvisun/kkdg/utils/kklog"
	"golang.org/x/sync/semaphore"
)

type clientConn struct {
	id    kknet.CONN_ID
	conn  net.Conn
	opts  kknet.Options
	stats *kknet.Stats

	sendMu      sync.Mutex
	sendData    *sync.Cond
	sendQueue   []*kkbuffer.ByteBuffer
	sendHead    int
	sendTail    int
	sendCount   int
	sendLimit   int
	sendClosed  bool
	sendDrain   bool
	spaceSem    *semaphore.Weighted
	spaceCtx    context.Context
	spaceCancel context.CancelFunc
	closeOnce   sync.Once

	ctxMu sync.RWMutex
	ctx   context.Context
}

var _ kknet.IConn = (*clientConn)(nil)

func newClientConn(conn net.Conn, opts kknet.Options, stats *kknet.Stats) *clientConn {
	queueLimit := opts.WriteBufferSize
	if queueLimit < opts.MaxMessageSize {
		queueLimit = opts.MaxMessageSize
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
		sendQueue:   make([]*kkbuffer.ByteBuffer, queueSize),
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
	if len(bb.B) > c.opts.MaxMessageSize {
		if c.stats != nil {
			c.stats.AddError()
		}
		return kkerrors.ErrMaxMessageSize
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
	if c.sendCount == len(c.sendQueue) {
		c.growSendQueueLocked()
	}
	c.sendQueue[c.sendTail] = bb
	c.sendTail = (c.sendTail + 1) % len(c.sendQueue)
	c.sendCount++
	c.sendData.Signal()
	c.sendMu.Unlock()
	return nil
}

func (c *clientConn) Send(data []byte) error {
	if len(data) > c.opts.MaxMessageSize {
		if c.stats != nil {
			c.stats.AddError()
		}
		return kkerrors.ErrMaxMessageSize
	}
	bb := kkbuffer.GetWithCapacity(len(data))
	bb.B = bb.B[:len(data)]
	copy(bb.B, data)
	return c.SendBuffer(bb)
}

func (c *clientConn) Close() error {
	c.sendMu.Lock()
	c.sendClosed = true
	c.sendDrain = c.opts.TcpClientNeedFlushOver
	flushTimeout := c.opts.TimeoutTcpFlushOver
	flushCb := c.opts.TcpClientFlushTimeoutCallback
	c.sendData.Broadcast()
	c.sendMu.Unlock()
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
			c.sendMu.Lock()
			if c.sendClosed && c.sendDrain {
				shouldCallback = flushCb != nil
				c.sendDrain = false
				c.sendData.Broadcast()
				if c.stats != nil {
					c.stats.AddError()
				}
				c.sendMu.Unlock()
				if shouldCallback {
					flushCb(c, flushTimeout)
				}
				_ = c.conn.Close()
				kklog.Warnf("tcp client flush timeout: %v", flushTimeout)
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
	header := make([]byte, 4)
	for {
		if err := readFull(c.conn, header); err != nil {
			return err
		}
		size := int(kkpacket.GetByteOrder().Uint32(header))
		if size < 0 || size > c.opts.MaxMessageSize {
			return kkerrors.ErrMaxMessageSize
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
		c.sendMu.Lock()
		c.sendClosed = true
		c.sendDrain = false
		c.sendData.Broadcast()
		c.sendMu.Unlock()
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
		for c.sendCount == 0 && !c.sendClosed {
			c.sendData.Wait()
		}
		if c.sendClosed && !c.sendDrain {
			c.sendMu.Unlock()
			return nil
		}
		if c.sendCount == 0 {
			if c.sendClosed && c.sendDrain {
				c.sendDrain = false
				c.sendMu.Unlock()
				_ = c.conn.Close()
				return nil
			}
			c.sendMu.Unlock()
			continue
		}
		bb := c.sendQueue[c.sendHead]
		c.sendQueue[c.sendHead] = nil
		c.sendHead = (c.sendHead + 1) % len(c.sendQueue)
		c.sendCount--
		if c.sendCount == 0 {
			c.sendHead = 0
			c.sendTail = 0
		}
		c.spaceSem.Release(int64(len(bb.B)))
		c.sendMu.Unlock()

		err := writeFull(c.conn, bb.B)
		kkbuffer.Put(bb)
		if err != nil {
			return err
		}
		if c.stats != nil {
			c.stats.AddSent(len(bb.B))
		}

	}
}

func (c *clientConn) growSendQueueLocked() {
	newSize := len(c.sendQueue) * 2
	if newSize == 0 {
		newSize = 64
	}
	newQueue := make([]*kkbuffer.ByteBuffer, newSize)
	if c.sendCount > 0 {
		if c.sendHead < c.sendTail {
			copy(newQueue, c.sendQueue[c.sendHead:c.sendTail])
		} else {
			n := copy(newQueue, c.sendQueue[c.sendHead:])
			copy(newQueue[n:], c.sendQueue[:c.sendTail])
		}
	}
	c.sendQueue = newQueue
	c.sendHead = 0
	c.sendTail = c.sendCount
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
