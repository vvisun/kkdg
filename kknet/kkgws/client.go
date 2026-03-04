package kkgws

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/lxzan/gws"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// Client represents a WebSocket client backed by gws.
type Client struct {
	url     string
	handler kknet.IConnLifecycleHandler
	opts    kknet.Options

	connMu       sync.Mutex
	conn         *gwsConn
	connected    atomic.Bool
	reconnecting atomic.Bool
	closing      atomic.Bool
	stopCh       chan struct{}

	stats kknet.Stats
}

var _ kknet.IClient = (*Client)(nil)

// NewClient creates a new gws-based WebSocket client.
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

	gc, done, err := c.dialAndStart()
	if err != nil {
		c.connected.Store(false)
		c.stats.AddError()
		return err
	}

	go func() {
		<-done
		c.connMu.Lock()
		if c.conn == gc {
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

func (c *Client) Addr() string {
	return c.url
}

func (c *Client) dialAndStart() (*gwsConn, <-chan struct{}, error) {
	socket, _, err := gws.NewClient(&gwsClientEventHandler{client: c}, &gws.ClientOption{
		Addr:            c.url,
		ReadBufferSize:  c.opts.ReadBufferSize,
		WriteBufferSize: c.opts.WriteBufferSize,
		TlsConfig:       c.opts.TLSConfig,
	})
	if err != nil {
		return nil, nil, err
	}

	socket.SetNoDelay(true)
	gc := newGwsConn(socket, &c.opts, &c.stats)
	socket.Session().Store(sessionKeyConn, gc)

	c.connMu.Lock()
	c.conn = gc
	c.connMu.Unlock()

	c.stats.OnConnect()
	if c.handler != nil {
		kknet.SafeHandlerCall(c.opts.Logger, &c.stats, "kkgws client OnConnect", func() {
			c.handler.OnConnect(gc)
		})
	}

	gc.startPingByTimingWheel()
	if c.opts.ReadTimeout > 0 {
		_ = socket.SetReadDeadline(time.Now().Add(c.opts.ReadTimeout))
	}

	done := make(chan struct{})
	go func() {
		socket.ReadLoop()
		close(done)
	}()

	return gc, done, nil
}

func (c *Client) startReconnectLoop(first chan<- error) {
	if c.reconnecting.Swap(true) {
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
			c.opts.Logger.Warnf("kkgws client reconnect failed. attempts exceeded. err: %v", err)
			reportFirst(err)
			return
		}

		attempts++
		gc, done, err := c.dialAndStart()
		if err == nil {
			consecutiveFails = 0
			if cb != nil {
				cb(attempts, nil)
			}
			reportFirst(nil)

			select {
			case <-done:
			case <-c.stopCh:
				_ = gc.Close()
				return
			}

			c.connMu.Lock()
			if c.conn == gc {
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
				c.opts.Logger.Warnf("kkgws client reconnect failed. attempts exceeded. err: %v", err)
				reportFirst(err)
				return
			}
		}

		delay := kknet.ReconnectBackoff(baseInterval, maxInterval, consecutiveFails)
		c.opts.Logger.Debugf("kkgws client reconnect backoff %v (consecutive fails: %d)", delay, consecutiveFails)
		select {
		case <-time.After(delay):
		case <-c.stopCh:
			reportFirst(kkerrors.ErrClientNotConnected)
			return
		}
	}
}

// --- gws client event handler ---

type gwsClientEventHandler struct {
	gws.BuiltinEventHandler
	client *Client
}

func (h *gwsClientEventHandler) OnOpen(socket *gws.Conn) {
	_ = socket.SetNoDelay(true)
}

func (h *gwsClientEventHandler) OnClose(socket *gws.Conn, err error) {
	c := getGwsConn(socket)
	if c == nil {
		return
	}
	c.doClose(h.client.handler, err)
}

func (h *gwsClientEventHandler) OnPing(socket *gws.Conn, payload []byte) {
	_ = socket.WritePong(payload)
	if h.client.opts.ReadTimeout > 0 {
		_ = socket.SetReadDeadline(time.Now().Add(h.client.opts.ReadTimeout))
	}
}

func (h *gwsClientEventHandler) OnPong(socket *gws.Conn, payload []byte) {
	if h.client.opts.ReadTimeout > 0 {
		_ = socket.SetReadDeadline(time.Now().Add(h.client.opts.ReadTimeout))
	}
}

func (h *gwsClientEventHandler) OnMessage(socket *gws.Conn, message *gws.Message) {
	defer message.Close()

	if message.Opcode != gws.OpcodeBinary {
		return
	}

	c := getGwsConn(socket)
	if c == nil {
		return
	}
	c.onRecvMessage(message.Bytes())
}
