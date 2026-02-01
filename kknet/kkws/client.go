package kkws

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers"
)

// Client represents a WebSocket client.
type Client struct {
	url     string
	handler kknet.IConnLifecycleHandler
	opts    kknet.Options

	connMu    sync.Mutex
	conn      *wsConn
	connected atomic.Bool

	stats kknet.Stats
}

var _ kknet.IClient = (*Client)(nil)

// NewClient creates a new WebSocket client.
func NewClient(url string, handler kknet.IConnLifecycleHandler, opts ...kknet.Option) *Client {
	return &Client{
		url:     url,
		handler: handler,
		opts:    kknet.ApplyOptions(opts...),
	}
}

// Connect connects to the server.
func (c *Client) Connect() error {
	if c.connected.Swap(true) {
		return nil
	}

	dialer := websocket.Dialer{
		ReadBufferSize:  c.opts.ReadBufferSize,
		WriteBufferSize: c.opts.WriteBufferSize,
		TLSClientConfig: c.opts.TLSConfig,
	}

	conn, _, err := dialer.Dial(c.url, nil)
	if err != nil {
		c.connected.Store(false)
		c.stats.AddError()
		return err
	}
	conn.SetReadLimit(int64(kkpacket.DefaultMaxMessageSize()))

	// Set read/write timeouts if configured
	if c.opts.WsReadTimeout > 0 {
		if err := conn.SetReadDeadline(time.Now().Add(c.opts.WsReadTimeout)); err != nil {
			c.stats.AddError()
			c.opts.Logger.Warnf("kkws client set read deadline error: %v", err)
		}
	}
	if c.opts.WsWriteTimeout > 0 {
		if err := conn.SetWriteDeadline(time.Now().Add(c.opts.WsWriteTimeout)); err != nil {
			c.stats.AddError()
			c.opts.Logger.Warnf("kkws client set write deadline error: %v", err)
		}
	}

	wsConn := newWSConn(conn, c.opts, &c.stats)

	c.connMu.Lock()
	c.conn = wsConn
	c.connMu.Unlock()

	c.stats.OnConnect()
	if c.handler != nil {
		kknet.SafeHandlerCall(c.opts.Logger, &c.stats, "kkws OnConnect", func() {
			c.handler.OnConnect(wsConn)
		})
	}

	go func() {
		err := wsConn.readLoop()
		wsConn.closeWithError(c.handler, err)
		c.connected.Store(false)
	}()

	return nil
}

func (c *Client) SendBuffer(buffer buffers.IBuffer) error {
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
	if c.conn == nil {
		return nil
	}
	return c.conn
}

// Stats returns a snapshot of client statistics.
func (c *Client) Stats() kknet.StatsSnapshot {
	return c.stats.Snapshot()
}

func (c *Client) SetContext(ctx context.Context) {
	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()
	if conn != nil {
		conn.SetContext(ctx)
	}
}

func (c *Client) Addr() string {
	return c.url
}
