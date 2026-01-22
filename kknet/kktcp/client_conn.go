package kktcp

import (
	"context"
	"io"
	"net"
	"sync"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/buffers/kkring"
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
	sendClosed bool
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
	c.sendCond.Broadcast()
	c.sendMu.Unlock()
	return c.conn.Close()
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
	var header [4]byte
	headerRead := 0
	payloadRemaining := 0
	currentMsgLen := 0

	for {
		c.sendMu.Lock()
		for c.sendBuf.IsEmpty() && !c.sendClosed {
			c.sendCond.Wait()
		}
		if c.sendClosed {
			c.sendMu.Unlock()
			return nil
		}
		buffered := c.sendBuf.Buffered()
		if buffered == 0 {
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
		if err == nil && c.stats != nil {
			n := len(chunk.B)
			for i := 0; i < n; {
				if payloadRemaining > 0 {
					remain := n - i
					if remain >= payloadRemaining {
						i += payloadRemaining
						payloadRemaining = 0
						c.stats.AddSent(currentMsgLen)
						continue
					}
					payloadRemaining -= remain
					break
				}
				if headerRead < 4 {
					need := 4 - headerRead
					remain := n - i
					if remain < need {
						copy(header[headerRead:], chunk.B[i:])
						headerRead += remain
						break
					}
					copy(header[headerRead:], chunk.B[i:i+need])
					i += need
					headerRead = 4
					currentMsgLen = int(kkpacket.GetByteOrder().Uint32(header[:]))
					payloadRemaining = currentMsgLen
					headerRead = 0
					if payloadRemaining == 0 {
						c.stats.AddSent(0)
						continue
					}
				}
			}
		}
		kkbuffer.Put(chunk)
		if err != nil {
			return err
		}

		c.sendMu.Lock()
		_, _ = c.sendBuf.Discard(buffered)
		c.sendCond.Broadcast()
		c.sendMu.Unlock()
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
