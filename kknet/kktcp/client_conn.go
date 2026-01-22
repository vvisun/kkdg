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
)

type clientConn struct {
	id    kknet.CONN_ID
	conn  net.Conn
	opts  kknet.Options
	stats *kknet.Stats

	sendMu     sync.Mutex
	sendCond   *sync.Cond
	sendQueue  []*kkbuffer.ByteBuffer
	sendHead   int
	sendQueued int
	sendLimit  int
	sendClosed bool
	sendDrain  bool
	closeOnce  sync.Once

	ctxMu sync.RWMutex
	ctx   context.Context
}

var _ kknet.IConn = (*clientConn)(nil)

func newClientConn(conn net.Conn, opts kknet.Options, stats *kknet.Stats) *clientConn {
	queueLimit := opts.WriteBufferSize
	if queueLimit < opts.MaxMessageSize {
		queueLimit = opts.MaxMessageSize
	}
	cc := &clientConn{
		id:        kknet.NextConnID(),
		conn:      conn,
		opts:      opts,
		stats:     stats,
		sendLimit: queueLimit,
		ctx:       context.Background(),
	}
	cc.sendCond = sync.NewCond(&cc.sendMu)
	return cc
}

func (c *clientConn) ID() kknet.CONN_ID {
	return c.id
}

func (c *clientConn) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}

func (c *clientConn) SendBuffer(bb buffers.IBuffer) error {
	// as bb is not used in other places, we can just use it directly
	// but maybe other places did Put(bb), so we need to get a new one
	// just swap, no need to copy. avoid other put(bb) and reuse, make bb dirty.
	oldBB := bb
	bb = kkbuffer.GetWithCapacity(len(oldBB.B))
	bb.B, oldBB.B = oldBB.B, bb.B
	kkbuffer.Put(oldBB)

	c.sendMu.Lock()
	if len(bb.B) > c.sendLimit {
		c.sendLimit = len(bb.B)
	}
	for !c.sendClosed && c.sendQueued+len(bb.B) > c.sendLimit {
		c.sendCond.Wait()
	}
	if c.sendClosed {
		c.sendMu.Unlock()
		kkbuffer.Put(bb)
		if c.stats != nil {
			c.stats.AddError()
		}
		return kkerrors.ErrConnectionClosed
	}
	c.sendQueue = append(c.sendQueue, bb)
	c.sendQueued += len(bb.B)
	c.sendCond.Signal()
	c.sendMu.Unlock()
	return nil
}

func (c *clientConn) Send(data []byte) error {
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
	c.sendCond.Broadcast()
	c.sendMu.Unlock()
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
				c.sendCond.Broadcast()
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
		c.sendCond.Broadcast()
		c.sendMu.Unlock()
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
		for c.sendHead >= len(c.sendQueue) && !c.sendClosed {
			c.sendCond.Wait()
		}
		if c.sendClosed && !c.sendDrain {
			c.sendMu.Unlock()
			return nil
		}
		if c.sendHead >= len(c.sendQueue) {
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
		c.sendHead++
		c.sendQueued -= len(bb.B)
		c.compactSendQueueLocked()
		c.sendCond.Broadcast()
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

func (c *clientConn) compactSendQueueLocked() {
	if c.sendHead > 0 && c.sendHead*2 >= len(c.sendQueue) {
		c.sendQueue = append([]*kkbuffer.ByteBuffer(nil), c.sendQueue[c.sendHead:]...)
		c.sendHead = 0
	}
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
