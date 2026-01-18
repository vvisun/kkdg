package kkws

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/gorilla/websocket"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// Client represents a WebSocket client.
type Client struct {
	url     string
	handler kknet.IHandler
	opts    kknet.Options

	connMu    sync.Mutex
	conn      *wsConn
	connected atomic.Bool

	stats kknet.Stats
}

// NewClient creates a new WebSocket client.
func NewClient(url string, handler kknet.IHandler, opts ...kknet.Option) *Client {
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
		err := wsConn.readLoop(func(conn kknet.IConn, data buffers.IBuffer) {
			if c.handler != nil {
				c.handler.OnMessage(conn, data)
			}
			kkbuffer.Put(data)
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
	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()
	if conn != nil {
		conn.SetContext(ctx)
	}
}

type wsConn struct {
	id    int64
	conn  *websocket.Conn
	opts  kknet.Options
	stats *kknet.Stats

	writeMu   sync.Mutex
	closeOnce sync.Once

	ctxMu sync.RWMutex
	ctx   context.Context
}

func newWSConn(conn *websocket.Conn, opts kknet.Options, stats *kknet.Stats) *wsConn {
	return &wsConn{
		id:    kknet.NextConnID(),
		conn:  conn,
		opts:  opts,
		stats: stats,
		ctx:   context.Background(),
	}
}

func (c *wsConn) ID() int64 {
	return c.id
}

func (c *wsConn) RemoteAddr() string {
	if c.conn == nil || c.conn.UnderlyingConn() == nil {
		return ""
	}
	return c.conn.UnderlyingConn().RemoteAddr().String()
}

func (c *wsConn) Send(data []byte) error {
	if len(data) > c.opts.MaxMessageSize {
		if c.stats != nil {
			c.stats.AddError()
		}
		return kkerrors.ErrMaxMessageSize
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if err := c.conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
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

func (c *wsConn) Close() error {
	c.closeWithError(nil, nil)
	return nil
}

func (c *wsConn) Context() context.Context {
	c.ctxMu.RLock()
	defer c.ctxMu.RUnlock()
	return c.ctx
}

func (c *wsConn) SetContext(ctx context.Context) {
	c.ctxMu.Lock()
	c.ctx = ctx
	c.ctxMu.Unlock()
}

func (c *wsConn) readLoop(dispatch func(kknet.IConn, buffers.IBuffer)) error {
	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			return err
		}
		if c.stats != nil {
			c.stats.AddRecv(len(data))
		}
		payload := kkbuffer.Get()
		payload.Set(data)
		if dispatch != nil {
			dispatch(c, payload)
		} else {
			kkbuffer.Put(payload)
		}
	}
}

func (c *wsConn) closeWithError(handler kknet.IHandler, err error) {
	c.closeOnce.Do(func() {
		if c.stats != nil {
			c.stats.OnClose()
			if err != nil {
				c.stats.AddError()
			}
		}
		_ = c.conn.Close()
		if handler != nil {
			handler.OnClose(c, err)
		}
	})
}
