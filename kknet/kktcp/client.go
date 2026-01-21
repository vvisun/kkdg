package kktcp

import (
	"crypto/tls"
	"net"
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
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
