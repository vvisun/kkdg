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
	handler kknet.IConnLifecycleHandler
	opts    kknet.Options

	connMu    sync.Mutex
	conn      *clientConn
	connected atomic.Bool

	stats kknet.Stats
}

var _ kknet.IClient = (*Client)(nil)

// NewClient creates a new UDP client.
func NewClient(addr string, handler kknet.IConnLifecycleHandler, opts kknet.Options) *Client {
	kknet.CheckOptions(&opts)
	return &Client{
		addr:    addr,
		handler: handler,
		opts:    opts,
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
		err := cc.readLoop()
		cc.closeWithError(c.handler, err)
		c.connected.Store(false)
	}()
	return nil
}

func (c *Client) SendMsg(msg any) error {
	if msg == nil {
		return kkerrors.ErrInvalidPacket
	}
	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()
	return conn.SendMsg(msg)
}

func (c *Client) SendBuffer(buffer *kkbuffer.ByteBuffer) error {
	if buffer == nil {
		return kkerrors.ErrInvalidPacket
	}
	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()
	if conn == nil {
		return kkerrors.ErrClientNotConnected
	}
	return conn.SendBuffer(buffer)
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

func (c *Client) SetContext(ctx context.Context) {

}

func (c *Client) Addr() string {
	return c.addr
}
