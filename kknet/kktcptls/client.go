package kktcptls

import (
	"crypto/tls"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

const dialTimeout = 10 * time.Second

// Client represents a TCP/TLS client based on standard library net.
type Client struct {
	addr    string
	handler kknet.IConnLifecycleHandler
	opts    kknet.Options

	connMu       sync.Mutex
	conn         *tlsConn
	connected    atomic.Bool
	reconnecting atomic.Bool
	closing      atomic.Bool
	stopCh       chan struct{}

	stats kknet.Stats
}

var _ kknet.IClient = (*Client)(nil)

// NewClient creates a new TCP/TLS client.
// When opts.TLSConfig is set, connections use TLS; otherwise plain TCP.
func NewClient(addr string, handler kknet.IConnLifecycleHandler, opts kknet.Options) *Client {
	kknet.CheckOptions(&opts)
	return &Client{
		addr:    addr,
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
	tc, done, err := c.dialAndStart()
	if err != nil {
		c.connected.Store(false)
		c.stats.AddError()
		return err
	}

	go func() {
		<-done
		c.connMu.Lock()
		if c.conn == tc {
			c.conn = nil
		}
		c.connMu.Unlock()
		c.connected.Store(false)
	}()
	return nil
}

func (c *Client) IsStopped() bool {
	return c.closing.Load()
}

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

func (c *Client) SendBuffer(buffer *kkbuffer.ByteBuffer) error {
	if buffer == nil {
		return kkerrors.ErrInvalidPacket
	}
	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()
	if conn == nil {
		kkbuffer.Put(buffer)
		return kkerrors.ErrClientNotConnected
	}
	return conn.SendBuffer(buffer)
}

// Conn returns the underlying connection (may be nil).
func (c *Client) Conn() kknet.IConn {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if c.conn == nil {
		return nil
	}
	return c.conn
}

func (c *Client) Addr() string               { return c.addr }
func (c *Client) Stats() kknet.StatsSnapshot { return c.stats.Snapshot() }

// -------------------- dial --------------------

func (c *Client) dial() (net.Conn, error) {
	if c.opts.TLSConfig != nil {
		return tls.DialWithDialer(
			&net.Dialer{Timeout: dialTimeout},
			"tcp", c.addr, c.opts.TLSConfig,
		)
	}
	return net.DialTimeout("tcp", c.addr, dialTimeout)
}

func (c *Client) dialAndStart() (*tlsConn, <-chan struct{}, error) {
	rawConn, err := c.dial()
	if err != nil {
		return nil, nil, err
	}
	setTCPKeepAlive(rawConn)

	tc := newTLSConn(rawConn, &c.opts, &c.stats)

	c.connMu.Lock()
	c.conn = tc
	c.connMu.Unlock()

	c.stats.OnConnect()
	if c.handler != nil {
		kknet.SafeHandlerCall(c.opts.Logger, &c.stats, "kktcptls OnConnect", func() {
			c.handler.OnConnect(tc)
		})
	}

	done := make(chan struct{})
	go func() {
		err := tc.readLoop()
		tc.closeWithError(c.handler, err)
		close(done)
	}()

	return tc, done, nil
}

// -------------------- reconnect --------------------

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
			c.opts.Logger.Warnf("kktcptls client reconnect failed. attempts exceeded. err: %v", err)
			reportFirst(err)
			return
		}

		attempts++
		tc, done, err := c.dialAndStart()
		if err == nil {
			consecutiveFails = 0
			if cb != nil {
				cb(attempts, nil)
			}
			reportFirst(nil)

			select {
			case <-done:
			case <-c.stopCh:
				_ = tc.Close()
				return
			}

			c.connMu.Lock()
			if c.conn == tc {
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
				c.opts.Logger.Warnf("kktcptls client reconnect failed. attempts exceeded. err: %v", err)
				reportFirst(err)
				return
			}
		}

		delay := kknet.ReconnectBackoff(baseInterval, maxInterval, consecutiveFails)
		c.opts.Logger.Debugf("kktcptls client reconnect backoff %v (consecutive fails: %d)", delay, consecutiveFails)
		select {
		case <-time.After(delay):
		case <-c.stopCh:
			reportFirst(kkerrors.ErrClientNotConnected)
			return
		}
	}
}
