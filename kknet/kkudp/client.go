package kkudp

import (
	"context"
	"net"
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// Client represents a UDP client.
type Client struct {
	addr    string
	handler kknet.IHandler
	opts    kknet.Options

	connMu    sync.Mutex
	conn      *clientConn
	connected atomic.Bool

	stats kknet.Stats
}

// NewClient creates a new UDP client.
func NewClient(addr string, handler kknet.IHandler, opts ...kknet.Option) *Client {
	return &Client{
		addr:    addr,
		handler: handler,
		opts:    kknet.ApplyOptions(opts...),
	}
}

// Connect connects to the server address.
func (c *Client) Connect() error {
	if c.connected.Swap(true) {
		return nil
	}
	raddr, err := net.ResolveUDPAddr("udp", c.addr)
	if err != nil {
		c.connected.Store(false)
		return err
	}
	conn, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		c.connected.Store(false)
		return err
	}

	cc := newClientConn(conn, c.opts, &c.stats)
	c.connMu.Lock()
	c.conn = cc
	c.connMu.Unlock()

	c.stats.OnConnect()
	if c.handler != nil {
		kknet.SafeHandlerCall(c.opts.Logger, &c.stats, "udpclient OnConnect", func() {
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

// Send sends a datagram to the server.
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
	conn  *net.UDPConn
	opts  kknet.Options
	stats *kknet.Stats

	writeMu   sync.Mutex
	closeOnce sync.Once

	ctxMu sync.RWMutex
	ctx   context.Context
}

func newClientConn(conn *net.UDPConn, opts kknet.Options, stats *kknet.Stats) *clientConn {
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
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_, err := c.conn.Write(data)
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
	buf := make([]byte, c.opts.MaxMessageSize)
	for {
		n, err := c.conn.Read(buf)
		if err != nil {
			return err
		}
		if c.stats != nil {
			c.stats.AddRecv(n)
		}
		if handler != nil && n > 0 {
			payload := kkbuffer.GetWithCapacity(n)
			payload.B = payload.B[:n]
			copy(payload.B, buf[:n])
			kknet.SafeHandlerCall(c.opts.Logger, c.stats, "udpclient OnMessage", func() {
				handler.OnMessage(c, payload)
			})
			kkbuffer.Put(payload)
		}
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
			kknet.SafeHandlerCall(c.opts.Logger, c.stats, "udpclient OnClose", func() {
				handler.OnClose(c, err)
			})
		}
	})
}
