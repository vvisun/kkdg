package kkudp

import (
	"context"
	"net"
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
)

// Client represents a UDP client.
type Client struct {
	addr    string
	handler kknet.Handler
	opts    kknet.Options

	connMu    sync.Mutex
	conn      *clientConn
	connected atomic.Bool
}

// NewClient creates a new UDP client.
func NewClient(addr string, handler kknet.Handler, opts ...kknet.Option) *Client {
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

	cc := newClientConn(conn, c.opts)
	c.connMu.Lock()
	c.conn = cc
	c.connMu.Unlock()

	if c.handler != nil {
		c.handler.OnConnect(cc)
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
func (c *Client) Conn() kknet.Conn {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	return c.conn
}

type clientConn struct {
	id   int64
	conn *net.UDPConn
	opts kknet.Options

	writeMu  sync.Mutex
	closeOnce sync.Once

	ctxMu sync.RWMutex
	ctx   context.Context
}

func newClientConn(conn *net.UDPConn, opts kknet.Options) *clientConn {
	return &clientConn{
		id:   kknet.NextConnID(),
		conn: conn,
		opts: opts,
		ctx:  context.Background(),
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
		return kkerrors.ErrMaxMessageSize
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_, err := c.conn.Write(data)
	return err
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

func (c *clientConn) readLoop(handler kknet.Handler) error {
	buf := make([]byte, c.opts.MaxMessageSize)
	for {
		n, err := c.conn.Read(buf)
		if err != nil {
			return err
		}
		if handler != nil && n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])
			handler.OnMessage(c, data)
		}
	}
}

func (c *clientConn) closeWithError(handler kknet.Handler, err error) {
	c.closeOnce.Do(func() {
		_ = c.conn.Close()
		if handler != nil {
			handler.OnClose(c, err)
		}
	})
}
