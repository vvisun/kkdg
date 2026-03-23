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
	conn   *gnetConn
	openCh chan struct{}

	status  int32       // kknet.ConnStatus — 唯一状态源
	started atomic.Bool // gnet 引擎生命周期（与连接状态正交）
	stopCh  chan struct{}

	stats kknet.Stats
}

var _ kknet.IClient = (*GnetClient)(nil)

// NewClient creates a new gnet-based TCP client.
func NewClient(addr string, handler kknet.IConnLifecycleHandler, opts kknet.Options) *GnetClient {
	kknet.CheckOptions(&opts)
	if opts.TLSConfig != nil {
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
	if !kknet.IsConnected(&c.status) {
		return false
	}
	c.connMu.Lock()
	defer c.connMu.Unlock()
	return c.conn != nil
}

// Connect connects to the server and starts the gnet client engine.
// 状态转换：Init/Closed → Connecting → (OnOpen) → Connected
func (c *GnetClient) Connect() error {
	if kknet.IsConnected(&c.status) {
		return nil
	}
	// CAS 守卫：仅 Init 或 Closed 允许发起连接
	if !kknet.CASConnStatus(&c.status, kknet.ConnStatusInit, kknet.ConnStatusConnecting) &&
		!kknet.CASConnStatus(&c.status, kknet.ConnStatusClosed, kknet.ConnStatusConnecting) {
		return nil
	}

	select {
	case <-c.stopCh:
		c.stopCh = make(chan struct{})
	default:
	}
	if c.opts.TLSConfig != nil {
		kknet.ChangeConnStatus(&c.status, kknet.ConnStatusClosed)
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
			kknet.ChangeConnStatus(&c.status, kknet.ConnStatusClosed)
			c.opts.Logger.Infof("kktcp client start... failed to create client: %v", err)
			return err
		}
		if err := cli.Start(); err != nil {
			c.started.Store(false)
			kknet.ChangeConnStatus(&c.status, kknet.ConnStatusClosed)
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
		kknet.ChangeConnStatus(&c.status, kknet.ConnStatusClosed)
		c.opts.Logger.Infof("kktcp client start... failed to get client: %v", kkerrors.ErrNetClientNotConnected)
		return kkerrors.ErrNetClientNotConnected
	}

	if _, err := cli.Dial("tcp", c.addr); err != nil {
		kknet.ChangeConnStatus(&c.status, kknet.ConnStatusClosed)
		c.opts.Logger.Infof("kktcp client start... failed to dial: %v", err)
		return err
	}

	<-openCh
	if kknet.IsClosingOrClosed(&c.status) {
		return kkerrors.ErrNetConnectionClosed
	}
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
	return kknet.IsClosingOrClosed(&c.status)
}

// Close closes the client connection.
// 状态转换：any → Closing → Closed
func (c *GnetClient) Close() error {
	kknet.ChangeConnStatus(&c.status, kknet.ConnStatusClosing)

	c.connMu.Lock()
	conn := c.conn
	c.conn = nil
	openCh := c.openCh
	c.openCh = nil
	c.connMu.Unlock()

	if openCh != nil {
		select {
		case <-openCh:
		default:
			close(openCh)
		}
	}

	if conn == nil {
		kknet.ChangeConnStatus(&c.status, kknet.ConnStatusClosed)
		return kkerrors.ErrNetClientNotConnected
	}

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
	c.started.Store(false)
	kknet.ChangeConnStatus(&c.status, kknet.ConnStatusClosed)
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
	for {
		cur := kknet.LoadConnStatus(&c.status)
		if cur == kknet.ConnStatusReconnecting || cur >= kknet.ConnStatusClosing {
			return
		}
		if kknet.CASConnStatus(&c.status, cur, kknet.ConnStatusReconnecting) {
			go c.reconnectLoop()
			return
		}
	}
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
		if kknet.IsClosingOrClosed(&c.status) {
			return
		}
		if maxRetries > 0 && attempts >= maxRetries {
			if cb != nil {
				cb(attempts, kkerrors.ErrNetReconnectAttemptsExceeded)
			}
			c.opts.Logger.Warnf("gnetclient reconnect exceeded after %d attempts", attempts)
			kknet.ChangeConnStatus(&c.status, kknet.ConnStatusClosed)
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
			kknet.ChangeConnStatus(&c.status, kknet.ConnStatusClosed)
			return
		}

		if _, err := cli.Dial("tcp", c.addr); err == nil {
			select {
			case <-openCh:
				if cb != nil {
					cb(attempts, nil)
				}
				c.opts.Logger.Infof("gnetclient reconnected after %d attempts", attempts)
				return
			case <-c.stopCh:
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
			return
		}
	}
}
