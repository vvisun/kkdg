package kktcp

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// Client represents a TCP client.
type Client struct {
	addr    string
	handler kknet.IHandler
	opts    kknet.Options

	connMu    sync.Mutex
	conn      *clientConn
	connected atomic.Bool

	stats kknet.Stats
}

// NewClient creates a new TCP client.
func NewClient(addr string, handler kknet.IHandler, opts ...kknet.Option) *Client {
	return &Client{
		addr:    addr,
		handler: handler,
		opts:    kknet.ApplyOptions(opts...),
	}
}

// Connect connects to the server.
func (c *Client) Connect() error {
	if c.connected.Swap(true) {
		return nil
	}

	var conn net.Conn
	var err error
	if c.opts.TLSConfig != nil {
		conn, err = tls.Dial("tcp", c.addr, c.opts.TLSConfig)
	} else {
		conn, err = net.Dial("tcp", c.addr)
	}
	if err != nil {
		c.connected.Store(false)
		return err
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		if c.opts.ReadBufferSize > 0 {
			_ = tcp.SetReadBuffer(c.opts.ReadBufferSize)
		}
		if c.opts.WriteBufferSize > 0 {
			_ = tcp.SetWriteBuffer(c.opts.WriteBufferSize)
		}
	}

	cc := newClientConn(conn, c.opts, &c.stats)
	c.connMu.Lock()
	c.conn = cc
	c.connMu.Unlock()

	c.stats.OnConnect()
	if c.handler != nil {
		kknet.SafeHandlerCall(c.opts.Logger, &c.stats, "tcpclient OnConnect", func() {
			c.handler.OnConnect(cc)
		})
	}

	go func() {
		err := cc.readLoop(c.handler)
		cc.closeWithError(c.handler, err)
		c.connected.Store(false)
	}()

	return nil
}

// Send sends a message to the server.
func (c *Client) Send(data []byte) error {
	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()
	if conn == nil {
		return kkerrors.ErrClientNotConnected
	}
	return conn.Send(data)
}

// Close closes the client connection.
func (c *Client) Close() error {
	c.connMu.Lock()
	conn := c.conn
	c.conn = nil
	c.connMu.Unlock()
	if conn == nil {
		return kkerrors.ErrClientNotConnected
	}
	c.connected.Store(false)
	return conn.Close()
}

// Conn returns the underlying connection.
func (c *Client) Conn() kknet.IConn {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	return c.conn
}

// Stats returns a snapshot of client statistics.
func (c *Client) Stats() kknet.StatsSnapshot {
	return c.stats.Snapshot()
}

type clientConn struct {
	id    int64
	conn  net.Conn
	opts  kknet.Options
	stats *kknet.Stats

	writeMu   sync.Mutex
	closeOnce sync.Once

	ctxMu sync.RWMutex
	ctx   context.Context
}

func newClientConn(conn net.Conn, opts kknet.Options, stats *kknet.Stats) *clientConn {
	return &clientConn{
		id:    kknet.NextConnID(),
		conn:  conn,
		opts:  opts,
		stats: stats,
		ctx:   context.Background(),
	}
}

func (c *clientConn) ID() int64 {
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
	bb := kkbuffer.Get()
	bb.B = bb.B[:0]
	bb.B = append(bb.B, 0, 0, 0, 0)
	kknet.GetByteOrder().PutUint32(bb.B[:4], uint32(len(data)))
	bb.B = append(bb.B, data...)
	defer kkbuffer.Put(bb)

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	err := writeFull(c.conn, bb.B)
	if err != nil {
		if c.stats != nil {
			c.stats.AddError()
		}
		return err
	}
	if c.stats != nil {
		c.stats.AddSent(len(data))
	}
	return nil
}

func (c *clientConn) Close() error {
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
		size := int(binary.BigEndian.Uint32(header))
		if size < 0 || size > c.opts.MaxMessageSize {
			return kkerrors.ErrMaxMessageSize
		}
		payload := kkbuffer.Get()
		if cap(payload.B) < size {
			payload.B = make([]byte, size)
		} else {
			payload.B = payload.B[:size]
		}
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
