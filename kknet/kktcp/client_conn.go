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
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/buffers/kkring"
	"github.com/vvisun/kkdg/utils/kklog"
)

type clientConn struct {
	id    kknet.CONN_ID
	conn  net.Conn
	opts  kknet.Options
	stats *kknet.Stats

	sendMu     sync.Mutex
	sendCond   *sync.Cond
	sendBuf    *kkring.Buffer
	sendLimit  int
	sendMeta   []sendMeta
	sendHead   int
	sendWire   int
	sendPayLen int
	sendClosed bool
	sendDrain  bool
	closeOnce  sync.Once

	ctxMu sync.RWMutex
	ctx   context.Context
}

var _ kknet.IConn = (*clientConn)(nil)

func newClientConn(conn net.Conn, opts kknet.Options, stats *kknet.Stats) *clientConn {
	queueLimit := opts.WriteBufferSize
	minPacketSize := opts.MaxMessageSize + 4
	if queueLimit < minPacketSize {
		queueLimit = minPacketSize
	}
	cc := &clientConn{
		id:        kknet.NextConnID(),
		conn:      conn,
		opts:      opts,
		stats:     stats,
		sendBuf:   kkring.New(queueLimit),
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

func (c *clientConn) Send(data []byte) error {
	if len(data) > c.opts.MaxMessageSize {
		if c.stats != nil {
			c.stats.AddError()
		}
		return kkerrors.ErrMaxMessageSize
	}

	bb, err1 := c.opts.StreamPacket.Pack(data, c.opts.MaxMessageSize)
	if err1 != nil {
		kkbuffer.Put(bb)
		if c.stats != nil {
			c.stats.AddError()
		}
		return err1
	}

	c.sendMu.Lock()
	for !c.sendClosed && c.sendBuf.Available() < len(bb.B) {
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
	_, err := c.sendBuf.Write(bb.B)
	c.appendSendMetaLocked(len(bb.B), len(data))
	c.sendCond.Signal()
	c.sendMu.Unlock()
	kkbuffer.Put(bb)
	if err != nil {
		if c.stats != nil {
			c.stats.AddError()
		}
		return err
	}
	return nil
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
			c.sendMu.Lock()
			if c.sendClosed && c.sendDrain {
				if flushCb != nil {
					flushCb(c, flushTimeout)
				}
				if c.stats != nil {
					c.stats.AddError()
				}
				c.sendDrain = false
				c.sendCond.Broadcast()
				c.sendMu.Unlock()
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
		for c.sendBuf.IsEmpty() && !c.sendClosed {
			c.sendCond.Wait()
		}
		if c.sendClosed && !c.sendDrain {
			c.sendMu.Unlock()
			return nil
		}
		buffered := c.sendBuf.Buffered()
		if buffered == 0 {
			if c.sendClosed && c.sendDrain {
				c.sendDrain = false
				c.sendMu.Unlock()
				_ = c.conn.Close()
				return nil
			}
			c.sendMu.Unlock()
			continue
		}
		if buffered > c.sendLimit {
			buffered = c.sendLimit
		}
		head, tail := c.sendBuf.Peek(buffered)
		chunk := kkbuffer.GetWithCapacity(buffered)
		chunk.B = chunk.B[:buffered]
		copy(chunk.B, head)
		if len(tail) > 0 {
			copy(chunk.B[len(head):], tail)
		}
		c.sendMu.Unlock()

		err := writeFull(c.conn, chunk.B)
		kkbuffer.Put(chunk)
		if err != nil {
			return err
		}

		c.sendMu.Lock()
		_, _ = c.sendBuf.Discard(buffered)
		c.consumeWrittenLocked(buffered)
		c.sendCond.Broadcast()
		c.sendMu.Unlock()
	}
}

type sendMeta struct {
	wireLen    int
	payloadLen int
}

func (c *clientConn) appendSendMetaLocked(wireLen, payloadLen int) {
	if wireLen <= 0 {
		return
	}
	c.sendMeta = append(c.sendMeta, sendMeta{wireLen: wireLen, payloadLen: payloadLen})
	c.compactSendMetaLocked()
}

func (c *clientConn) consumeWrittenLocked(written int) {
	for written > 0 {
		if c.sendWire == 0 {
			if c.sendHead >= len(c.sendMeta) {
				break
			}
			meta := c.sendMeta[c.sendHead]
			c.sendHead++
			c.sendWire = meta.wireLen
			c.sendPayLen = meta.payloadLen
		}
		if written >= c.sendWire {
			written -= c.sendWire
			c.sendWire = 0
			if c.stats != nil {
				c.stats.AddSent(c.sendPayLen)
			}
			c.sendPayLen = 0
			continue
		}
		c.sendWire -= written
		written = 0
	}
	c.compactSendMetaLocked()
}

func (c *clientConn) compactSendMetaLocked() {
	if c.sendHead > 0 && c.sendHead*2 >= len(c.sendMeta) {
		c.sendMeta = append([]sendMeta(nil), c.sendMeta[c.sendHead:]...)
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
