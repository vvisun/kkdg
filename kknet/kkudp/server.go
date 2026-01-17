package kkudp

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/panjf2000/ants/v2"
	"github.com/panjf2000/gnet/v2"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
)

// Server represents a UDP server.
type Server struct {
	addr    string
	handler kknet.Handler
	opts    kknet.Options

	engine  gnet.Engine
	pool    *ants.Pool
	started atomic.Bool
	booted  chan struct{}
	done    chan error

	conns   map[string]*udpConn
	connsMu sync.Mutex
}

// NewServer creates a new UDP server.
func NewServer(addr string, handler kknet.Handler, opts ...kknet.Option) *Server {
	return &Server{
		addr:    addr,
		handler: handler,
		opts:    kknet.ApplyOptions(opts...),
		conns:   make(map[string]*udpConn),
	}
}

// Start begins listening for datagrams.
func (s *Server) Start() error {
	if s.started.Swap(true) {
		return nil
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

	handler := &udpEventHandler{server: s}
	go func() {
		err := gnet.Run(handler, "udp://"+s.addr, gnet.WithMulticore(true))
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
	if err := s.engine.Stop(context.Background()); err != nil {
		return err
	}
	if s.done != nil {
		_ = <-s.done
	}
	s.closeAll(kkerrors.ErrServerNotStarted)
	if s.pool != nil {
		s.pool.Release()
		s.pool = nil
	}
	return nil
}

// Addr returns the server address.
func (s *Server) Addr() string {
	return s.addr
}

type udpEventHandler struct {
	*gnet.BuiltinEventEngine
	server *Server
}

func (h *udpEventHandler) OnBoot(eng gnet.Engine) (action gnet.Action) {
	h.server.engine = eng
	h.server.opts.Logger.Infof("kkudp server listen on %s", h.server.addr)
	close(h.server.booted)
	return gnet.None
}

func (h *udpEventHandler) OnTraffic(c gnet.Conn) (action gnet.Action) {
	buf := make([]byte, h.server.opts.MaxMessageSize)
	n, err := c.Read(buf)
	if err != nil {
		h.server.opts.Logger.Errorf("kkudp read error: %v", err)
		return gnet.Close
	}

	uc := h.server.getOrCreateConn(c)
	if h.server.handler != nil && n > 0 {
		payload := make([]byte, n)
		copy(payload, buf[:n])
		h.dispatch(uc, payload)
	}
	return gnet.None
}

func (h *udpEventHandler) dispatch(c *udpConn, data []byte) {
	if h.server.pool == nil {
		h.server.handler.OnMessage(c, data)
		return
	}
	if err := h.server.pool.Submit(func() {
		h.server.handler.OnMessage(c, data)
	}); err != nil {
		h.server.opts.Logger.Errorf("kkudp submit task error: %v", err)
	}
}

func (s *Server) getOrCreateConn(c gnet.Conn) *udpConn {
	key := c.RemoteAddr().String()
	s.connsMu.Lock()
	defer s.connsMu.Unlock()
	if conn, ok := s.conns[key]; ok {
		return conn
	}
	conn := newUDPConn(c, s.opts)
	s.conns[key] = conn
	if s.handler != nil {
		s.handler.OnConnect(conn)
	}
	return conn
}

func (s *Server) closeAll(err error) {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()
	for _, c := range s.conns {
		if s.handler != nil {
			s.handler.OnClose(c, err)
		}
	}
	s.conns = make(map[string]*udpConn)
}

type udpConn struct {
	id   int64
	conn gnet.Conn
	opts kknet.Options

	ctxMu sync.RWMutex
	ctx   context.Context
}

func newUDPConn(c gnet.Conn, opts kknet.Options) *udpConn {
	return &udpConn{
		id:   kknet.NextConnID(),
		conn: c,
		opts: opts,
		ctx:  context.Background(),
	}
}

func (c *udpConn) ID() int64 {
	return c.id
}

func (c *udpConn) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}

func (c *udpConn) Send(data []byte) error {
	if len(data) > c.opts.MaxMessageSize {
		return kkerrors.ErrMaxMessageSize
	}
	return c.conn.AsyncWrite(data, nil)
}

func (c *udpConn) Close() error {
	return c.conn.Close()
}

func (c *udpConn) Context() context.Context {
	c.ctxMu.RLock()
	defer c.ctxMu.RUnlock()
	return c.ctx
}

func (c *udpConn) SetContext(ctx context.Context) {
	c.ctxMu.Lock()
	c.ctx = ctx
	c.ctxMu.Unlock()
}
