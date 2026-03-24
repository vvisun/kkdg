package kkws

import (
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// Client represents a WebSocket client.
type Client struct {
	url     string
	handler kknet.IConnLifecycleHandler
	opts    kknet.Options

	connMu sync.Mutex
	conn   *wsConn
	status int32 // kknet.ConnStatus
	stopCh chan struct{}

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
	if !kknet.IsConnected(&c.status) {
		return false
	}
	c.connMu.Lock()
	defer c.connMu.Unlock()
	return c.conn != nil
}

// Connect connects to the server.
// 状态转换：Init/Closed → Connecting → Connected
func (c *Client) Connect() error {
	if kknet.IsConnected(&c.status) {
		return nil
	}
	if !kknet.CASConnStatus(&c.status, kknet.ConnStatusInit, kknet.ConnStatusConnecting) &&
		!kknet.CASConnStatus(&c.status, kknet.ConnStatusClosed, kknet.ConnStatusConnecting) {
		return nil
	}

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
				kknet.ChangeConnStatus(&c.status, kknet.ConnStatusClosed)
			}
			return err
		case <-c.stopCh:
			kknet.ChangeConnStatus(&c.status, kknet.ConnStatusClosed)
			return kkerrors.ErrNetClientNotConnected
		}
	}

	// Single attempt.
	wc, done, err := c.dialAndStart()
	if err != nil {
		kknet.ChangeConnStatus(&c.status, kknet.ConnStatusClosed)
		c.stats.AddError()
		return err
	}
	kknet.ChangeConnStatus(&c.status, kknet.ConnStatusConnected)

	go func() {
		<-done
		c.connMu.Lock()
		if c.conn == wc {
			c.conn = nil
		}
		c.connMu.Unlock()
		kknet.ChangeConnStatus(&c.status, kknet.ConnStatusClosed)
	}()
	return nil
}

func (c *Client) SendBuffer(buffer *kkbuffer.ByteBuffer) error {
	if buffer == nil {
		return kkerrors.ErrClusterInvalidPacket
	}
	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()
	if conn == nil {
		kkbuffer.Put(buffer)
		return kkerrors.ErrNetClientNotConnected
	}
	return conn.SendBuffer(buffer)
}

func (c *Client) SendMsg(msg any) error {
	if msg == nil {
		return kkerrors.ErrClusterInvalidPacket
	}
	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()
	if conn == nil {
		return kkerrors.ErrNetClientNotConnected
	}
	return conn.SendMsg(msg)
}

func (c *Client) IsStopped() bool {
	return kknet.IsClosingOrClosed(&c.status)
}

// Close closes the client connection.
// 状态转换：any → Closing → Closed
func (c *Client) Close() error {
	kknet.ChangeConnStatus(&c.status, kknet.ConnStatusClosing)
	c.connMu.Lock()
	conn := c.conn
	c.conn = nil
	c.connMu.Unlock()
	if conn == nil {
		kknet.ChangeConnStatus(&c.status, kknet.ConnStatusClosed)
		return kkerrors.ErrNetClientNotConnected
	}
	select {
	case <-c.stopCh:
	default:
		close(c.stopCh)
	}
	err := conn.Close()
	kknet.ChangeConnStatus(&c.status, kknet.ConnStatusClosed)
	return err
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
	conn.SetReadLimit(int64(c.opts.StreamTool.MaxPacketSize()))

	wsConn := newWSConn(conn, &c.opts, &c.stats, c.handler)

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
	cur := kknet.LoadConnStatus(&c.status)
	if cur == kknet.ConnStatusReconnecting || cur >= kknet.ConnStatusClosing {
		select {
		case first <- nil:
		default:
		}
		return
	}
	go c.reconnectLoop(first)
}

func (c *Client) reconnectLoop(first chan<- error) {
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
		if kknet.IsClosingOrClosed(&c.status) {
			reportFirst(kkerrors.ErrNetClientNotConnected)
			return
		}
		if maxRetries > 0 && attempts >= maxRetries {
			err := kkerrors.ErrNetReconnectAttemptsExceeded
			if cb != nil {
				cb(attempts, err)
			}
			c.opts.Logger.Warnf("kkws client reconnect failed. attempts exceeded. err: %v", err)
			reportFirst(err)
			kknet.ChangeConnStatus(&c.status, kknet.ConnStatusClosed)
			return
		}

		attempts++
		wc, done, err := c.dialAndStart()
		if err == nil {
			consecutiveFails = 0
			if cb != nil {
				cb(attempts, nil)
			}
			if firstReported {
				kknet.ChangeConnStatus(&c.status, kknet.ConnStatusReconnected)
			} else {
				kknet.ChangeConnStatus(&c.status, kknet.ConnStatusConnected)
			}
			reportFirst(nil)

			select {
			case <-done:
			case <-c.stopCh:
				_ = wc.Close()
				return
			}

			c.connMu.Lock()
			if c.conn == wc {
				c.conn = nil
			}
			c.connMu.Unlock()

			if kknet.IsClosingOrClosed(&c.status) {
				return
			}
			kknet.ChangeConnStatus(&c.status, kknet.ConnStatusReconnecting)
		} else {
			consecutiveFails++
			c.stats.AddError()
			if cb != nil {
				cb(attempts, err)
			}
			if maxRetries > 0 && attempts >= maxRetries {
				c.opts.Logger.Warnf("kkws client reconnect failed. attempts exceeded. err: %v", err)
				reportFirst(err)
				kknet.ChangeConnStatus(&c.status, kknet.ConnStatusClosed)
				return
			}
		}

		delay := kknet.ReconnectBackoff(baseInterval, maxInterval, consecutiveFails)
		c.opts.Logger.Debugf("kkws client reconnect backoff %v (consecutive fails: %d)", delay, consecutiveFails)
		select {
		case <-time.After(delay):
		case <-c.stopCh:
			reportFirst(kkerrors.ErrNetClientNotConnected)
			return
		}
	}
}
