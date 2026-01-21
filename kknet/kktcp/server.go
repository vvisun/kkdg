package kktcp

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/panjf2000/gnet/v2"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// Server represents a TCP server with length-prefixed messages.
type Server struct {
	addr    string
	handler kknet.IHandler
	opts    kknet.Options
	connMgr *kknet.ConnManager

	engine  gnet.Engine
	pool    *ants.Pool
	started atomic.Bool
	booted  chan struct{}
	done    chan error

	stats kknet.Stats
}

var _ kknet.IServer = (*Server)(nil)

// NewServer creates a new TCP server.
func NewServer(addr string, handler kknet.IHandler, opts ...kknet.Option) *Server {
	cfg := kknet.ApplyOptions(opts...)
	return &Server{
		addr:    addr,
		handler: handler,
		opts:    cfg,
		connMgr: kknet.NewConnManager(),
	}
}

// Start begins listening and accepting connections.
func (s *Server) Start() error {
	if s.started.Swap(true) {
		return nil
	}

	// Apply middlewares to handler
	s.handler = kknet.ApplyMiddlewares(s.handler, s.opts.Middlewares...)

	if s.opts.PoolSize > 0 {
		poolOpts := make([]ants.Option, 0, 1)
		if logger, ok := s.opts.Logger.(ants.Logger); ok {
			poolOpts = append(poolOpts, ants.WithLogger(logger))
		}
		pool, err := ants.NewPool(s.opts.PoolSize, poolOpts...)
		if err != nil {
			s.started.Store(false)
			return err
		}
		s.pool = pool
	}

	s.booted = make(chan struct{})
	s.done = make(chan error, 1)

	handler := &tcpEventHandler{server: s}
	go func() {
		err := gnet.Run(handler, "tcp://"+s.addr, gnet.WithMulticore(true))
		s.done <- err
	}()

	select {
	case <-s.booted:
		return nil
	case err := <-s.done:
		s.started.Store(false)
		if s.pool != nil {
			s.pool.Release()
			s.pool = nil
		}
		return err
	}
}

// Stop shuts down the server.
func (s *Server) Stop() error {
	if !s.started.Swap(false) {
		return kkerrors.ErrServerNotStarted
	}

	// Use timeout context for shutdown
	timeout := s.opts.ShutdownTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Stop the gnet engine with timeout
	if err := s.engine.Stop(ctx); err != nil {
		if ctx.Err() != nil {
			s.opts.Logger.Warnf("kktcp server shutdown timeout after %v", timeout)
		}
		return err
	}

	// Wait for done channel or timeout
	if s.done != nil {
		select {
		case <-s.done:
		case <-ctx.Done():
			s.opts.Logger.Warnf("kktcp server shutdown timeout after %v", timeout)
		}
	}

	if s.pool != nil {
		s.pool.Release()
		s.pool = nil
	}
	return nil
}

// Addr returns the server listening address.
func (s *Server) Addr() string {
	return s.addr
}

// Stats returns a snapshot of server statistics.
func (s *Server) Stats() kknet.StatsSnapshot {
	return s.stats.Snapshot()
}

// GetConnManager returns the connection manager.
func (s *Server) GetConnManager() kknet.IConnManager {
	return s.connMgr
}

type tcpEventHandler struct {
	*gnet.BuiltinEventEngine
	server *Server
}

func (h *tcpEventHandler) OnBoot(eng gnet.Engine) (action gnet.Action) {
	h.server.engine = eng
	h.server.opts.Logger.Infof("kktcp server listen on %s", h.server.addr)
	close(h.server.booted)
	return gnet.None
}

func (h *tcpEventHandler) OnShutdown(eng gnet.Engine) {
	_ = eng
}

func (h *tcpEventHandler) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	h.server.stats.OnConnect()
	tconn := newTCPConn(c, h.server.opts, &h.server.stats)
	h.server.connMgr.AddConn(tconn)
	c.SetContext(tconn)
	if h.server.handler != nil {
		kknet.SafeHandlerCall(h.server.opts.Logger, &h.server.stats, "kktcp OnConnect", func() {
			h.server.handler.OnConnect(tconn)
		})
	}
	return nil, gnet.None
}

func (h *tcpEventHandler) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	h.server.stats.OnClose()
	if err != nil {
		h.server.stats.AddError()
	}
	if tc, ok := c.Context().(*tcpConn); ok {
		h.server.connMgr.RemoveConn(tc.id)
	}
	if h.server.handler == nil {
		return gnet.None
	}
	if tc, ok := c.Context().(*tcpConn); ok {
		kknet.SafeHandlerCall(h.server.opts.Logger, &h.server.stats, "kktcp OnClose", func() {
			h.server.handler.OnClose(tc, err)
		})
	}
	return gnet.None
}

func (h *tcpEventHandler) OnTraffic(c gnet.Conn) (action gnet.Action) {
	tc, ok := c.Context().(*tcpConn)
	if !ok {
		return gnet.Close
	}

	for {
		data, ok, err := h.server.opts.StreamPacket.Unpack(c, h.server.opts.MaxMessageSize)
		if err != nil {
			h.server.stats.AddError()
			return gnet.Close
		}
		if !ok {
			return gnet.None
		}
		h.server.stats.AddRecv(len(data))
		if h.server.handler != nil {
			payload := kkbuffer.Get()
			payload.SetBytes(data)
			h.dispatch(tc, payload)
		}
	}
}

func (h *tcpEventHandler) dispatch(c *tcpConn, data buffers.IBuffer) {
	if h.server.pool == nil {
		defer kkbuffer.Put(data)
		kknet.SafeHandlerCall(h.server.opts.Logger, &h.server.stats, "kktcp OnMessage", func() {
			h.server.handler.OnMessage(c, data)
		})
		return
	}
	if err := h.server.pool.Submit(func() {
		defer kkbuffer.Put(data)
		kknet.SafeHandlerCall(h.server.opts.Logger, &h.server.stats, "kktcp OnMessage", func() {
			h.server.handler.OnMessage(c, data)
		})
	}); err != nil {
		kkbuffer.Put(data)
		h.server.opts.Logger.Errorf("kktcp submit task error: %v", err)
	}
}

type tcpConn struct {
	id    int64
	conn  gnet.Conn
	opts  kknet.Options
	stats *kknet.Stats

	ctxMu sync.RWMutex
	ctx   context.Context
}

var _ kknet.IConn = (*tcpConn)(nil)

func newTCPConn(c gnet.Conn, opts kknet.Options, stats *kknet.Stats) *tcpConn {
	return &tcpConn{
		id:    kknet.NextConnID(),
		conn:  c,
		opts:  opts,
		stats: stats,
		ctx:   context.Background(),
	}
}

func (c *tcpConn) ID() int64 {
	return c.id
}

func (c *tcpConn) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}

func (c *tcpConn) Send(data []byte) error {
	if len(data) > c.opts.MaxMessageSize {
		if c.stats != nil {
			c.stats.AddError()
		}
		return kkerrors.ErrMaxMessageSize
	}

	bb, err1 := c.opts.StreamPacket.Pack(data, c.opts.MaxMessageSize)
	if err1 != nil {
		kkbuffer.Put(bb)
		if c.stats != nil {
			c.stats.AddError()
		}
		return err1
	}

	err := c.conn.AsyncWrite(bb.B, func(_ gnet.Conn, _ error) error {
		kkbuffer.Put(bb)
		return nil
	})
	if err != nil {
		kkbuffer.Put(bb)
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

func (c *tcpConn) Close() error {
	return c.conn.Close()
}

func (c *tcpConn) Context() context.Context {
	c.ctxMu.RLock()
	defer c.ctxMu.RUnlock()
	return c.ctx
}

func (c *tcpConn) SetContext(ctx context.Context) {
	c.ctxMu.Lock()
	c.ctx = ctx
	c.ctxMu.Unlock()
}
