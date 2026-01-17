package kkws

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/gorilla/websocket"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
)

// Client represents a WebSocket client.
type Client struct {
	url     string
	handler kknet.Handler
	opts    kknet.Options

	connMu   sync.Mutex
	conn     *wsConn
	connected atomic.Bool

	stats kknet.Stats
}

// NewClient creates a new WebSocket client.
func NewClient(url string, handler kknet.Handler, opts ...kknet.Option) *Client {
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
	}

	conn, _, err := dialer.Dial(c.url, nil)
	if err != nil {
		c.connected.Store(false)
		c.stats.AddError()
		return err
	}
	conn.SetReadLimit(int64(c.opts.MaxMessageSize))

	wsConn := newWSConn(conn, c.opts, &c.stats)

	c.connMu.Lock()
	c.conn = wsConn
	c.connMu.Unlock()

	c.stats.OnConnect()
	if c.handler != nil {
		c.handler.OnConnect(wsConn)
	}

	go func() {
		err := wsConn.readLoop(func(conn kknet.Conn, data []byte) {
			if c.handler != nil {
				c.handler.OnMessage(conn, data)
			}
		})
		wsConn.closeWithError(c.handler, err)
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
