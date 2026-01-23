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
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// GnetClient represents a TCP client based on gnet.
type GnetClient struct {
	addr    string
	handler kknet.IHandler
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
func NewGnetClient(addr string, handler kknet.IHandler, opts ...kknet.Option) *GnetClient {
	return &GnetClient{
		addr:    addr,
		handler: handler,
		opts:    kknet.ApplyOptions(opts...),
		stopCh:  make(chan struct{}),
	}
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

// Send sends a message to the server.
func (c *GnetClient) Send(data []byte) error {
	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()
	if conn == nil {
		return kkerrors.ErrClientNotConnected
	}
	return conn.Send(data)
}

// SendBuffer sends a buffer to the server.
func (c *GnetClient) SendBuffer(buffer buffers.IBuffer) error {
	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()
	if conn == nil {
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

func (c *GnetClient) startReconnect() {
	if c.reconnecting.Swap(true) {
		return
	}
	go c.reconnectLoop()
}

func (c *GnetClient) reconnectLoop() {
	interval := c.opts.TcpClientReconnectInterval
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	maxRetries := c.opts.TcpClientReconnectMaxRetries
	cb := c.opts.TcpClientReconnectCallback
	attempts := 0
	for {
		if c.closing.Load() {
			c.reconnecting.Store(false)
			return
		}
		if maxRetries > 0 && attempts >= maxRetries {
			if cb != nil {
				cb(attempts, errors.New("reconnect attempts exceeded"))
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

type gnetClientEventHandler struct {
	*gnet.BuiltinEventEngine
	client *GnetClient
}

func (h *gnetClientEventHandler) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	h.client.stats.OnConnect()
	h.client.connected.Store(true)
	h.client.reconnecting.Store(false)
	cc := newGnetClientConn(c, h.client.opts, &h.client.stats)
	c.SetContext(cc)

	h.client.connMu.Lock()
	h.client.conn = cc
	openCh := h.client.openCh
	h.client.openCh = nil
	h.client.connMu.Unlock()
	if openCh != nil {
		close(openCh)
	}

	if h.client.handler != nil {
		kknet.SafeHandlerCall(h.client.opts.Logger, &h.client.stats, "gnetclient OnConnect", func() {
			h.client.handler.OnConnect(cc)
		})
	}
	return nil, gnet.None
}

func (h *gnetClientEventHandler) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	h.client.stats.OnClose()
	if err != nil {
		h.client.stats.AddError()
	}
	if cc, ok := c.Context().(*gnetClientConn); ok && h.client.handler != nil {
		kknet.SafeHandlerCall(h.client.opts.Logger, &h.client.stats, "gnetclient OnClose", func() {
			h.client.handler.OnClose(cc, err)
		})
	}
	h.client.connMu.Lock()
	h.client.conn = nil
	h.client.connMu.Unlock()
	h.client.connected.Store(false)
	if !h.client.closing.Load() && h.client.opts.TcpClientNeedReconnect {
		h.client.startReconnect()
	}
	return gnet.None
}

func (h *gnetClientEventHandler) OnTraffic(c gnet.Conn) (action gnet.Action) {
	cc, ok := c.Context().(*gnetClientConn)
	if !ok {
		return gnet.Close
	}
	for {
		data, ok, err := h.client.opts.StreamPacket.Unpack(c, kkpacket.DefaultMaxMessageSize())
		if err != nil {
			h.client.stats.AddError()
			return gnet.Close
		}
		if !ok {
			return gnet.None
		}
		h.client.stats.AddRecv(len(data))
		if h.client.handler != nil {
			payload := kkbuffer.Get()
			payload.SetBytes(data)
			kknet.SafeHandlerCall(h.client.opts.Logger, &h.client.stats, "gnetclient OnMessage", func() {
				h.client.handler.OnMessage(cc, payload)
			})
			kkbuffer.Put(payload)
		}
	}
}

type gnetClientConn struct {
	id    kknet.CONN_ID
	conn  gnet.Conn
	opts  kknet.Options
	stats *kknet.Stats

	ctxMu sync.RWMutex
	ctx   context.Context
}

var _ kknet.IConn = (*gnetClientConn)(nil)

func newGnetClientConn(c gnet.Conn, opts kknet.Options, stats *kknet.Stats) *gnetClientConn {
	return &gnetClientConn{
		id:    kknet.NextConnID(),
		conn:  c,
		opts:  opts,
		stats: stats,
		ctx:   context.Background(),
	}
}

func (c *gnetClientConn) ID() kknet.CONN_ID {
	return c.id
}

func (c *gnetClientConn) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}

func (c *gnetClientConn) SendBuffer(buffer buffers.IBuffer) error {
	if len(buffer.B) > kkpacket.DefaultMaxMessageSize() {
		if c.stats != nil {
			c.stats.AddError()
		}
		return kkerrors.ErrMaxMessageSize
	}
	old := buffer
	bb := kkbuffer.GetWithCapacity(len(old.B))
	bb.B, old.B = old.B, bb.B
	kkbuffer.Put(old)

	if err := c.conn.AsyncWrite(bb.B, func(_ gnet.Conn, err error) error {
		if err != nil {
			if c.stats != nil {
				c.stats.AddError()
			}
		} else if c.stats != nil {
			c.stats.AddSent(len(bb.B))
		}
		kkbuffer.Put(bb)
		return nil
	}); err != nil {
		kkbuffer.Put(bb)
		if c.stats != nil {
			c.stats.AddError()
		}
		return err
	}
	return nil
}

func (c *gnetClientConn) Send(data []byte) error {
	if len(data) > kkpacket.DefaultMaxMessageSize() {
		if c.stats != nil {
			c.stats.AddError()
		}
		return kkerrors.ErrMaxMessageSize
	}
	bb := kkbuffer.GetWithCapacity(len(data))
	bb.B = bb.B[:len(data)]
	copy(bb.B, data)
	return c.SendBuffer(bb)
}

func (c *gnetClientConn) Close() error {
	return c.conn.Close()
}

func (c *gnetClientConn) Context() context.Context {
	c.ctxMu.RLock()
	defer c.ctxMu.RUnlock()
	return c.ctx
}

func (c *gnetClientConn) SetContext(ctx context.Context) {
	c.ctxMu.Lock()
	c.ctx = ctx
	c.ctxMu.Unlock()
}
