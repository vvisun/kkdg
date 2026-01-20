package kktcp

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/panjf2000/gnet/v2"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

// Server represents a TCP server with length-prefixed messages.
type Server struct {
	addr    string
	handler kknet.IHandler
	opts    kknet.Options

	engine  gnet.Engine
	pool    *ants.Pool
	started atomic.Bool
	booted  chan struct{}
	done    chan error

	stats kknet.Stats

	tlsListener net.Listener
	tlsConns    map[int64]*tlsConn
	tlsMu       sync.Mutex
	tlsWg       sync.WaitGroup
}

// NewServer creates a new TCP server.
func NewServer(addr string, handler kknet.IHandler, opts ...kknet.Option) *Server {
	cfg := kknet.ApplyOptions(opts...)
	return &Server{
		addr:    addr,
		handler: handler,
		opts:    cfg,
	}
}

// Start begins listening and accepting connections.
func (s *Server) Start() error {
	if s.started.Swap(true) {
		return nil
	}

	// Apply middlewares to handler
	s.handler = kknet.ApplyMiddlewares(s.handler, s.opts.Middlewares...)

	if s.opts.TLSConfig != nil {
		return s.startTLS()
	}

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

	if s.opts.TLSConfig != nil {
		return s.stopTLSWithTimeout(ctx)
	}

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

func (s *Server) startTLS() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		s.started.Store(false)
		return err
	}
	s.tlsListener = tls.NewListener(ln, s.opts.TLSConfig)
	s.tlsConns = make(map[int64]*tlsConn)
	s.opts.Logger.Infof("kktcp tls server listen on %s", s.addr)

	s.tlsWg.Add(1)
	go s.acceptTLS()
	return nil
}

func (s *Server) stopTLSWithTimeout(ctx context.Context) error {
	if s.tlsListener != nil {
		_ = s.tlsListener.Close()
	}
	s.closeAllTLS()

	// Wait for all TLS connections to close with timeout
	done := make(chan struct{})
	go func() {
		s.tlsWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.opts.Logger.Warnf("kktcp tls server: some connections did not close within timeout")
		return ctx.Err()
	}
}

func (s *Server) acceptTLS() {
	defer s.tlsWg.Done()
	for {
		conn, err := s.tlsListener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			s.stats.AddError()
			s.opts.Logger.Errorf("kktcp tls accept error: %v", err)
			continue
		}
		s.handleTLSConn(conn)
	}
}

func (s *Server) handleTLSConn(conn net.Conn) {
	tc := newTLSConn(conn, s.opts, &s.stats)
	s.tlsMu.Lock()
	s.tlsConns[tc.id] = tc
	s.tlsMu.Unlock()

	s.stats.OnConnect()
	if s.handler != nil {
		kknet.SafeHandlerCall(s.opts.Logger, &s.stats, "kktcp OnConnect", func() {
			s.handler.OnConnect(tc)
		})
	}

	s.tlsWg.Add(1)
	go func() {
		defer s.tlsWg.Done()
		err := tc.readLoop(s.handler, s.opts.Logger)
		tc.closeWithError(s.handler, err)
		s.tlsMu.Lock()
		delete(s.tlsConns, tc.id)
		s.tlsMu.Unlock()
	}()
}

func (s *Server) closeAllTLS() {
	s.tlsMu.Lock()
	defer s.tlsMu.Unlock()
	for _, c := range s.tlsConns {
		c.closeWithError(s.handler, kkerrors.ErrServerStopped)
	}
	s.tlsConns = make(map[int64]*tlsConn)
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
		data, ok, err := h.server.opts.StreamPacket.Unpack(c)
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

	bb, err1 := c.opts.StreamPacket.Pack(data)
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

type tlsConn struct {
	id    int64
	conn  net.Conn
	opts  kknet.Options
	stats *kknet.Stats

	writeMu   sync.Mutex
	closeOnce sync.Once

	ctxMu sync.RWMutex
	ctx   context.Context
}

func newTLSConn(conn net.Conn, opts kknet.Options, stats *kknet.Stats) *tlsConn {
	return &tlsConn{
		id:    kknet.NextConnID(),
		conn:  conn,
		opts:  opts,
		stats: stats,
		ctx:   context.Background(),
	}
}

func (c *tlsConn) ID() int64 {
	return c.id
}

func (c *tlsConn) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}

func (c *tlsConn) Send(data []byte) error {
	if len(data) > c.opts.MaxMessageSize {
		if c.stats != nil {
			c.stats.AddError()
		}
		return kkerrors.ErrMaxMessageSize
	}

	bb, err1 := c.opts.StreamPacket.Pack(data)
	if err1 != nil {
		kkbuffer.Put(bb)
		if c.stats != nil {
			c.stats.AddError()
		}
		return err1
	}

	defer kkbuffer.Put(bb)

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	err := writeFull(c.conn, bb.B)
	if err != nil {
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

func (c *tlsConn) Close() error {
	return c.conn.Close()
}

func (c *tlsConn) Context() context.Context {
	c.ctxMu.RLock()
	defer c.ctxMu.RUnlock()
	return c.ctx
}

func (c *tlsConn) SetContext(ctx context.Context) {
	c.ctxMu.Lock()
	c.ctx = ctx
	c.ctxMu.Unlock()
}

func (c *tlsConn) readLoop(handler kknet.IHandler, logger kklog.ILogger) error {
	header := make([]byte, 4)
	for {
		if err := readFull(c.conn, header); err != nil {
			return err
		}
		size := int(kkpacket.GetByteOrder().Uint32(header))
		if size < 0 || size > c.opts.MaxMessageSize {
			if c.stats != nil {
				c.stats.AddError()
			}
			return kkerrors.ErrMaxMessageSize
		}
		payload := kkbuffer.GetWithCapacity(size)
		payload.B = payload.B[:size]
		if err := readFull(c.conn, payload.B); err != nil {
			kkbuffer.Put(payload)
			return err
		}
		if c.stats != nil {
			c.stats.AddRecv(len(payload.B))
		}
		if handler != nil {
			kknet.SafeHandlerCall(logger, c.stats, "kktcp OnMessage", func() {
				handler.OnMessage(c, payload)
			})
			kkbuffer.Put(payload)
			continue
		}
		kkbuffer.Put(payload)
	}
}

func (c *tlsConn) closeWithError(handler kknet.IHandler, err error) {
	c.closeOnce.Do(func() {
		if c.stats != nil {
			c.stats.OnClose()
			if err != nil {
				c.stats.AddError()
			}
		}
		_ = c.conn.Close()
		if handler != nil {
			kknet.SafeHandlerCall(c.opts.Logger, c.stats, "kktcp OnClose", func() {
				handler.OnClose(c, err)
			})
		}
	})
}
