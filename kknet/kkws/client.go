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
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// Client represents a WebSocket client.
type Client struct {
	url     string
	handler kknet.IConnLifecycleHandler
	opts    kknet.Options

	connMu       sync.Mutex
	conn         *wsConn
	connected    atomic.Bool
	reconnecting atomic.Bool
	closing      atomic.Bool
	stopCh       chan struct{}

	stats kknet.Stats
}

var _ kknet.IClient = (*Client)(nil)

// NewClient creates a new WebSocket client.
func NewClient(url string, handler kknet.IConnLifecycleHandler, opts kknet.Options) *Client {
	kknet.CheckOptions(&opts)
	return &Client{
		url:     url,
		handler: handler,
		opts:    opts,
		stopCh:  make(chan struct{}),
	}
}

func (c *Client) IsConnected() bool {
	if !c.connected.Load() {
		return false
	}
	c.connMu.Lock()
	defer c.connMu.Unlock()
	return c.conn != nil
}

// Connect connects to the server.
func (c *Client) Connect() error {
	if c.connected.Swap(true) {
		return nil
	}
	c.closing.Store(false)
	select {
	case <-c.stopCh:
		c.stopCh = make(chan struct{})
	default:
	}

	if c.opts.IsNeedReconnect {
		first := make(chan error, 1)
		c.startReconnectLoop(first)
		select {
		case err := <-first:
			if err != nil {
				c.connected.Store(false)
			}
			return err
		case <-c.stopCh:
			c.connected.Store(false)
			return kkerrors.ErrClientNotConnected
		}
	}

	// Single attempt.
	wc, done, err := c.dialAndStart()
	if err != nil {
		c.connected.Store(false)
		c.stats.AddError()
		return err
	}

	// Wait for disconnect in background; in non-reconnect mode we just mark disconnected.
	go func() {
		<-done
		c.connMu.Lock()
		if c.conn == wc {
			c.conn = nil
		}
		c.connMu.Unlock()
		c.connected.Store(false)
	}()
	return nil
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

func (c *Client) SendMsg(msg any) error {
	if msg == nil {
		return kkerrors.ErrInvalidPacket
	}
	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()
	if conn == nil {
		return kkerrors.ErrClientNotConnected
	}
	return conn.SendMsg(msg)
}

// Close closes the client connection.
func (c *Client) Close() error {
	c.connMu.Lock()
	conn := c.conn
	c.conn = nil
	c.connMu.Unlock()
	c.closing.Store(true)
	c.connected.Store(false)
	c.reconnecting.Store(false)
	select {
	case <-c.stopCh:
	default:
		close(c.stopCh)
	}
	if conn == nil {
		return kkerrors.ErrClientNotConnected
	}
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

func (c *Client) dialAndStart() (*wsConn, <-chan struct{}, error) {
	dialer := websocket.Dialer{
		ReadBufferSize:  c.opts.ReadBufferSize,
		WriteBufferSize: c.opts.WriteBufferSize,
		TLSClientConfig: c.opts.TLSConfig,
	}

	conn, _, err := dialer.Dial(c.url, nil)
	if err != nil {
		return nil, nil, err
	}
	conn.SetReadLimit(int64(kkpacket.MaxPacketSize()))

	wsConn := newWSConn(conn, &c.opts, &c.stats)

	c.connMu.Lock()
	c.conn = wsConn
	c.connMu.Unlock()

	c.stats.OnConnect()
	if c.handler != nil {
		kknet.SafeHandlerCall(c.opts.Logger, &c.stats, "kkws OnConnect", func() {
			c.handler.OnConnect(wsConn)
		})
	}

	done := make(chan struct{})
	go func() {
		err := wsConn.readLoop()
		wsConn.closeWithError(c.handler, err)
		close(done)
	}()

	return wsConn, done, nil
}

func (c *Client) startReconnectLoop(first chan<- error) {
	if c.reconnecting.Swap(true) {
		// already running
		select {
		case first <- nil:
		default:
		}
		return
	}
	go c.reconnectLoop(first)
}

func (c *Client) reconnectLoop(first chan<- error) {
	defer c.reconnecting.Store(false)

	baseInterval := c.opts.ReconnectInterval
	if baseInterval < 500*time.Millisecond {
		baseInterval = 500 * time.Millisecond
	}
	maxInterval := c.opts.ReconnectMaxInterval
	maxRetries := c.opts.ReconnectMaxRetries
	cb := c.opts.ReconnectCallback
	attempts := 0
	consecutiveFails := 0
	firstReported := false
	reportFirst := func(err error) {
		if firstReported || first == nil {
			return
		}
		firstReported = true
		select {
		case first <- err:
		default:
		}
	}

	for {
		if c.closing.Load() {
			reportFirst(kkerrors.ErrClientNotConnected)
			return
		}
		if maxRetries > 0 && attempts >= maxRetries {
			err := kkerrors.ErrReconnectAttemptsExceeded
			if cb != nil {
				cb(attempts, err)
			}
			c.opts.Logger.Warnf("kkws client reconnect failed. attempts exceeded. err: %v", err)
			reportFirst(err)
			return
		}

		attempts++
		wc, done, err := c.dialAndStart()
		if err == nil {
			consecutiveFails = 0
			if cb != nil {
				cb(attempts, nil)
			}
			reportFirst(nil)

			// Wait for disconnect, then loop again for reconnect.
			select {
			case <-done:
			case <-c.stopCh:
				_ = wc.Close()
				return
			}

			// clear conn pointer if it still points to this connection
			c.connMu.Lock()
			if c.conn == wc {
				c.conn = nil
			}
			c.connMu.Unlock()

			if c.closing.Load() {
				return
			}
		} else {
			consecutiveFails++
			c.stats.AddError()
			if cb != nil {
				cb(attempts, err)
			}
			if maxRetries > 0 && attempts >= maxRetries {
				c.opts.Logger.Warnf("kkws client reconnect failed. attempts exceeded. err: %v", err)
				reportFirst(err)
				return
			}
		}

		delay := kknet.ReconnectBackoff(baseInterval, maxInterval, consecutiveFails)
		c.opts.Logger.Debugf("kkws client reconnect backoff %v (consecutive fails: %d)", delay, consecutiveFails)
		select {
		case <-time.After(delay):
		case <-c.stopCh:
			reportFirst(kkerrors.ErrClientNotConnected)
			return
		}
	}
}
