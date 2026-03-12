package kktcp

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/panjf2000/gnet/v2"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

// GnetClient represents a TCP client based on gnet.
type GnetClient struct {
	addr    string
	handler kknet.IConnLifecycleHandler
	opts    kknet.Options

	clientMu sync.Mutex
	client   *gnet.Client

	connMu sync.Mutex
	conn   *gnetClientConn
	openCh chan struct{}

	connected    atomic.Bool
	started      atomic.Bool
	reconnecting atomic.Bool
	closing      atomic.Bool
	stopCh       chan struct{}

	stats kknet.Stats
}

var _ kknet.IClient = (*GnetClient)(nil)

// NewGnetClient creates a new gnet-based TCP client.
func NewClient(addr string, handler kknet.IConnLifecycleHandler, opts kknet.Options) *GnetClient {
	kknet.CheckOptions(&opts)
	if opts.TLSConfig != nil {
		// panic as gnet client does not support TLS
		kklog.PanicLog("gnet client does not support TLS. use kktcptls instead.")
	}
	return &GnetClient{
		addr:    addr,
		handler: handler,
		opts:    opts,
		stopCh:  make(chan struct{}),
	}
}

func (c *GnetClient) IsConnected() bool {
	if !c.connected.Load() {
		return false
	}
	c.connMu.Lock()
	defer c.connMu.Unlock()
	return c.conn != nil
}

// Connect connects to the server and starts the gnet client engine.
func (c *GnetClient) Connect() error {
	if c.connected.Swap(true) {
		return nil
	}
	c.closing.Store(false)
	select {
	case <-c.stopCh:
		c.stopCh = make(chan struct{})
	default:
	}
	if c.opts.TLSConfig != nil {
		c.connected.Store(false)
		return errors.New("gnet client does not support TLS")
	}

	if !c.started.Swap(true) {
		c.opts.Logger.Infof("kktcp client start... connecting to %s", c.addr)
		ev := &gnetClientEventHandler{client: c}
		cli, err := gnet.NewClient(ev,
			gnet.WithMulticore(false),
			gnet.WithNumEventLoop(1),
			gnet.WithLogger(gnetNopLogger),
			gnet.WithTCPKeepAlive(10*time.Second),
			gnet.WithReadBufferCap(c.opts.ReadBufferSize),
			gnet.WithWriteBufferCap(c.opts.WriteBufferSize),
			gnet.WithTCPNoDelay(gnet.TCPNoDelay),
		)
		if err != nil {
			c.started.Store(false)
			c.connected.Store(false)
			c.opts.Logger.Infof("kktcp client start... failed to create client: %v", err)
			return err
		}
		if err := cli.Start(); err != nil {
			c.started.Store(false)
			c.connected.Store(false)
			c.opts.Logger.Infof("kktcp client start... failed to start client: %v", err)
			return err
		}
		c.clientMu.Lock()
		c.client = cli
		c.clientMu.Unlock()
	}

	openCh := make(chan struct{})
	c.connMu.Lock()
	c.openCh = openCh
	c.connMu.Unlock()

	c.clientMu.Lock()
	cli := c.client
	c.clientMu.Unlock()
	if cli == nil {
		c.connected.Store(false)
		c.opts.Logger.Infof("kktcp client start... failed to get client: %v", kkerrors.ErrNetClientNotConnected)
		return kkerrors.ErrNetClientNotConnected
	}

	if _, err := cli.Dial("tcp", c.addr); err != nil {
		c.connected.Store(false)
		c.opts.Logger.Infof("kktcp client start... failed to dial: %v", err)
		return err
	}

	<-openCh
	return nil
}

func (c *GnetClient) SendMsg(msg any) error {
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

// SendBuffer sends a buffer to the server.
func (c *GnetClient) SendBuffer(buffer *kkbuffer.ByteBuffer) error {
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

func (c *GnetClient) IsStopped() bool {
	return c.closing.Load()
}

// Close closes the client connection.
func (c *GnetClient) Close() error {
	c.connMu.Lock()
	conn := c.conn
	c.conn = nil
	c.connMu.Unlock()
	if conn == nil {
		return kkerrors.ErrNetClientNotConnected
	}
	c.closing.Store(true)
	c.connected.Store(false)
	c.reconnecting.Store(false)
	c.opts.Logger.Infof("kktcp client close... closing connection")
	select {
	case <-c.stopCh:
	default:
		close(c.stopCh)
	}
	_ = conn.Close()
	c.clientMu.Lock()
	cli := c.client
	c.client = nil
	c.clientMu.Unlock()
	if cli != nil {
		_ = cli.Stop()
	}
	c.opts.Logger.Infof("kktcp client close... done")
	return nil
}

// Addr returns the remote address.
func (c *GnetClient) Addr() string {
	return c.addr
}

// Stats returns a snapshot of client statistics.
func (c *GnetClient) Stats() kknet.StatsSnapshot {
	return c.stats.Snapshot()
}

func (c *GnetClient) startReconnect() {
	if c.reconnecting.Swap(true) {
		return
	}
	go c.reconnectLoop()
}

func (c *GnetClient) reconnectLoop() {
	baseInterval := c.opts.ReconnectInterval
	if baseInterval < 500*time.Millisecond {
		baseInterval = 500 * time.Millisecond
	}
	maxInterval := c.opts.ReconnectMaxInterval
	maxRetries := c.opts.ReconnectMaxRetries
	cb := c.opts.ReconnectCallback
	attempts := 0
	consecutiveFails := 0
	for {
		if c.closing.Load() {
			c.reconnecting.Store(false)
			return
		}
		if maxRetries > 0 && attempts >= maxRetries {
			if cb != nil {
				cb(attempts, kkerrors.ErrNetReconnectAttemptsExceeded)
			}
			c.opts.Logger.Warnf("gnetclient reconnect exceeded after %d attempts", attempts)
			c.reconnecting.Store(false)
			return
		}
		attempts++
		c.opts.Logger.Debugf("gnetclient reconnect attempt %d", attempts)

		openCh := make(chan struct{})
		c.connMu.Lock()
		c.openCh = openCh
		c.connMu.Unlock()

		c.clientMu.Lock()
		cli := c.client
		c.clientMu.Unlock()
		if cli == nil {
			c.reconnecting.Store(false)
			return
		}

		if _, err := cli.Dial("tcp", c.addr); err == nil {
			select {
			case <-openCh:
				if cb != nil {
					cb(attempts, nil)
				}
				c.opts.Logger.Infof("gnetclient reconnected after %d attempts", attempts)
				c.reconnecting.Store(false)
				return
			case <-c.stopCh:
				c.reconnecting.Store(false)
				return
			}
		} else {
			consecutiveFails++
			if cb != nil {
				cb(attempts, err)
			}
			c.opts.Logger.Warnf("gnetclient reconnect attempt %d failed: %v", attempts, err)
		}

		delay := kknet.ReconnectBackoff(baseInterval, maxInterval, consecutiveFails)
		c.opts.Logger.Debugf("gnetclient reconnect backoff %v (consecutive fails: %d)", delay, consecutiveFails)
		select {
		case <-time.After(delay):
		case <-c.stopCh:
			c.reconnecting.Store(false)
			return
		}
	}
}
