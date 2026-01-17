package kktcp

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
)

// Client represents a TCP client.
type Client struct {
	addr    string
	handler kknet.Handler
	opts    kknet.Options

	connMu    sync.Mutex
	conn      *clientConn
	connected atomic.Bool
}

// NewClient creates a new TCP client.
func NewClient(addr string, handler kknet.Handler, opts ...kknet.Option) *Client {
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

	conn, err := net.Dial("tcp", c.addr)
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
func (c *Client) Conn() kknet.Conn {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	return c.conn
}

type clientConn struct {
	id   int64
	conn net.Conn
	opts kknet.Options

	writeMu  sync.Mutex
	closeOnce sync.Once

	ctxMu sync.RWMutex
	ctx   context.Context
}

func newClientConn(conn net.Conn, opts kknet.Options) *clientConn {
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
	buf := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(data)))
	copy(buf[4:], data)

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	return writeFull(c.conn, buf)
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
	header := make([]byte, 4)
	for {
		if err := readFull(c.conn, header); err != nil {
			return err
		}
		size := int(binary.BigEndian.Uint32(header))
		if size < 0 || size > c.opts.MaxMessageSize {
			return kkerrors.ErrMaxMessageSize
		}
		buf := make([]byte, size)
		if err := readFull(c.conn, buf); err != nil {
			return err
		}
		if handler != nil {
			handler.OnMessage(c, buf)
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
