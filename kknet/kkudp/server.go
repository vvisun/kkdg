package kkudp

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/panjf2000/gnet/v2"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// Server represents a UDP server.
type Server struct {
	addr    string
	handler kknet.IHandler
	opts    kknet.Options

	engine  gnet.Engine
	started atomic.Bool
	booted  chan struct{}
	done    chan error

	conns       map[string]*udpConn
	connsMu     sync.Mutex
	middlewares []kknet.Middleware

	stats kknet.Stats

	cleanupStop chan struct{}
	cleanupDone chan struct{}
}

// NewServer creates a new UDP server.
func NewServer(addr string, handler kknet.IHandler, opts ...kknet.Option) *Server {
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

	// Apply middlewares to handler
	s.handler = kknet.ApplyMiddlewares(s.handler, s.middlewares...)

	s.booted = make(chan struct{})
	s.done = make(chan error, 1)

	handler := &udpEventHandler{server: s}
	go func() {
		err := gnet.Run(handler, "udp://"+s.addr, gnet.WithMulticore(true))
		s.done <- err
	}()

	select {
	case <-s.booted:
		s.startCleanup()
		return nil
	case err := <-s.done:
		s.started.Store(false)
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
	s.stopCleanup()
	s.closeAll(kkerrors.ErrServerStopped)
	return nil
}

// Addr returns the server address.
func (s *Server) Addr() string {
	return s.addr
}

// Stats returns a snapshot of server statistics.
func (s *Server) Stats() kknet.StatsSnapshot {
	return s.stats.Snapshot()
}

// Use adds middleware to the server.
// Middlewares are applied in the order they are added.
// Call before Start.
func (s *Server) Use(middlewares ...kknet.Middleware) {
	if len(middlewares) == 0 {
		return
	}
	s.middlewares = append(s.middlewares, middlewares...)
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
	size := c.InboundBuffered()
	if size <= 0 {
		return gnet.None
	}
	if size > h.server.opts.MaxMessageSize {
		h.server.stats.AddError()
		h.server.opts.Logger.Errorf("kkudp message too large: %d", size)
		return gnet.None
	}
	data, err := c.Next(size)
	if err != nil {
		h.server.stats.AddError()
		h.server.opts.Logger.Errorf("kkudp read error: %v", err)
		return gnet.None
	}

	uc := h.server.newConn(c)
	h.server.stats.AddRecv(len(data))
	if h.server.handler != nil {
		payload := kkbuffer.Get()
		payload.SetBytes(data)
		h.dispatch(uc, payload)
	}
	uc.deactivate()
	return gnet.None
}

func (h *udpEventHandler) dispatch(c *udpConn, data buffers.IBuffer) {
	// UDP connection is only valid during OnTraffic callback.
	defer kkbuffer.Put(data)
	kknet.SafeHandlerCall(h.server.opts.Logger, &h.server.stats, "kkudp OnMessage", func() {
		h.server.handler.OnMessage(c, data)
	})
}

func (s *Server) newConn(c gnet.Conn) *udpConn {
	key := c.RemoteAddr().String()
	now := time.Now()
	s.connsMu.Lock()
	conn, exists := s.conns[key]
	if !exists {
		conn = newUDPConn(c, s.opts, &s.stats, now)
		s.conns[key] = conn
	}
	conn.activate(c, now)
	s.connsMu.Unlock()

	if !exists {
		s.stats.OnConnect()
		if s.handler != nil {
			kknet.SafeHandlerCall(s.opts.Logger, &s.stats, "kkudp OnConnect", func() {
				s.handler.OnConnect(conn)
			})
		}
	}
	return conn
}

func (s *Server) closeAll(err error) {
	s.connsMu.Lock()
	conns := make([]*udpConn, 0, len(s.conns))
	for _, conn := range s.conns {
		conns = append(conns, conn)
	}
	s.conns = make(map[string]*udpConn)
	s.connsMu.Unlock()
	for _, conn := range conns {
		s.closeConn(conn, err)
	}
}

type udpConn struct {
	id         int64
	conn       gnet.Conn
	remoteAddr string
	opts       kknet.Options
	active     atomic.Bool
	stats      *kknet.Stats
	lastSeen   atomic.Int64

	ctxMu sync.RWMutex
	ctx   context.Context
}

func newUDPConn(c gnet.Conn, opts kknet.Options, stats *kknet.Stats, now time.Time) *udpConn {
	conn := &udpConn{
		id:         kknet.NextConnID(),
		conn:       c,
		remoteAddr: c.RemoteAddr().String(),
		opts:       opts,
		stats:      stats,
		ctx:        context.Background(),
	}
	conn.active.Store(true)
	conn.lastSeen.Store(now.UnixNano())
	return conn
}

func (c *udpConn) ID() int64 {
	return c.id
}

func (c *udpConn) RemoteAddr() string {
	return c.remoteAddr
}

func (c *udpConn) Send(data []byte) error {
	if !c.active.Load() {
		return kkerrors.ErrConnectionClosed
	}
	if len(data) > c.opts.MaxMessageSize {
		if c.stats != nil {
			c.stats.AddError()
		}
		return kkerrors.ErrMaxMessageSize
	}
	if c.conn == nil {
		if c.stats != nil {
			c.stats.AddError()
		}
		return kkerrors.ErrConnectionClosed
	}
	_, err := c.conn.Write(data)
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

func (c *udpConn) Close() error {
	return nil
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

func (c *udpConn) deactivate() {
	c.active.Store(false)
}

func (c *udpConn) activate(conn gnet.Conn, now time.Time) {
	c.conn = conn
	c.remoteAddr = conn.RemoteAddr().String()
	c.active.Store(true)
	c.lastSeen.Store(now.UnixNano())
}

func (c *udpConn) isIdle(now time.Time, timeout time.Duration) bool {
	if c.active.Load() {
		return false
	}
	last := c.lastSeen.Load()
	if last == 0 {
		return false
	}
	return now.Sub(time.Unix(0, last)) >= timeout
}

func (s *Server) closeConn(conn *udpConn, err error) {
	s.stats.OnClose()
	if s.handler == nil {
		return
	}
	kknet.SafeHandlerCall(s.opts.Logger, &s.stats, "kkudp OnClose", func() {
		s.handler.OnClose(conn, err)
	})
}

func (s *Server) startCleanup() {
	if s.opts.UDPConnIdleTimeout <= 0 || s.opts.UDPCleanupInterval <= 0 {
		return
	}
	if s.cleanupStop != nil {
		return
	}
	s.cleanupStop = make(chan struct{})
	s.cleanupDone = make(chan struct{})
	go s.cleanupLoop()
}

func (s *Server) stopCleanup() {
	if s.cleanupStop == nil {
		return
	}
	close(s.cleanupStop)
	<-s.cleanupDone
	s.cleanupStop = nil
	s.cleanupDone = nil
}

func (s *Server) cleanupLoop() {
	defer close(s.cleanupDone)
	ticker := time.NewTicker(s.opts.UDPCleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.pruneIdle()
		case <-s.cleanupStop:
			return
		}
	}
}

func (s *Server) pruneIdle() {
	timeout := s.opts.UDPConnIdleTimeout
	if timeout <= 0 {
		return
	}
	now := time.Now()
	var idle []*udpConn
	s.connsMu.Lock()
	for key, conn := range s.conns {
		if conn.isIdle(now, timeout) {
			delete(s.conns, key)
			idle = append(idle, conn)
		}
	}
	s.connsMu.Unlock()
	for _, conn := range idle {
		s.closeConn(conn, kkerrors.ErrConnectionClosed)
	}
}
