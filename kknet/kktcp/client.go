package kktcp

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/panjf2000/gnet/v2"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
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
		ev := &gnetClientEventHandler{client: c}
		cli, err := gnet.NewClient(ev,
			gnet.WithReadBufferCap(c.opts.ReadBufferSize),
			gnet.WithWriteBufferCap(c.opts.WriteBufferSize),
			gnet.WithLogger(gnetNopLogger),
		)
		if err != nil {
			c.started.Store(false)
			c.connected.Store(false)
			return err
		}
		if err := cli.Start(); err != nil {
			c.started.Store(false)
			c.connected.Store(false)
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
		return kkerrors.ErrClientNotConnected
	}

	if _, err := cli.Dial("tcp", c.addr); err != nil {
		c.connected.Store(false)
		return err
	}

	<-openCh
	return nil
}

func (c *GnetClient) SendMsg(msg any) error {
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

// SendBuffer sends a buffer to the server.
func (c *GnetClient) SendBuffer(buffer *kkbuffer.ByteBuffer) error {
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

// Close closes the client connection.
func (c *GnetClient) Close() error {
	c.connMu.Lock()
	conn := c.conn
	c.conn = nil
	c.connMu.Unlock()
	if conn == nil {
		return kkerrors.ErrClientNotConnected
	}
	c.closing.Store(true)
	c.connected.Store(false)
	c.reconnecting.Store(false)
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

func (c *GnetClient) SetContext(ctx context.Context) {

}

func (c *GnetClient) startReconnect() {
	if c.reconnecting.Swap(true) {
		return
	}
	go c.reconnectLoop()
}

func (c *GnetClient) reconnectLoop() {
	interval := c.opts.ReconnectInterval
	if interval < 500*time.Millisecond { // 最小间隔，防止频繁重连
		interval = 500 * time.Millisecond
	}
	maxRetries := c.opts.ReconnectMaxRetries
	cb := c.opts.ReconnectCallback
	attempts := 0
	for {
		if c.closing.Load() {
			c.reconnecting.Store(false)
			return
		}
		if maxRetries > 0 && attempts >= maxRetries {
			if cb != nil {
				cb(attempts, kkerrors.ErrReconnectAttemptsExceeded)
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
			if cb != nil {
				cb(attempts, err)
			}
			c.opts.Logger.Warnf("gnetclient reconnect attempt %d failed: %v", attempts, err)
		}

		select {
		case <-time.After(interval):
		case <-c.stopCh:
			c.reconnecting.Store(false)
			return
		}
	}
}
