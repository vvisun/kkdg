package kkudp

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/panjf2000/gnet/v2"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// Server represents a UDP server.
type Server struct {
	addr    string
	handler kknet.Handler
	opts    kknet.Options

	engine  gnet.Engine
	started atomic.Bool
	booted  chan struct{}
	done    chan error

	seen    map[string]struct{}
	connsMu sync.Mutex

	stats kknet.Stats
}

// NewServer creates a new UDP server.
func NewServer(addr string, handler kknet.Handler, opts ...kknet.Option) *Server {
	return &Server{
		addr:    addr,
		handler: handler,
		opts:    kknet.ApplyOptions(opts...),
		seen:    make(map[string]struct{}),
	}
}

// Start begins listening for datagrams.
func (s *Server) Start() error {
	if s.started.Swap(true) {
		return nil
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
		payload.B = append(payload.B[:0], data...)
		h.dispatch(uc, payload)
	}
	uc.deactivate()
	return gnet.None
}

func (h *udpEventHandler) dispatch(c *udpConn, data buffers.IBuffer) {
	// UDP connection is only valid during OnTraffic callback.
	h.server.handler.OnMessage(c, data)
	releaseBuffer(data)
}

func (s *Server) newConn(c gnet.Conn) *udpConn {
	key := c.RemoteAddr().String()
	s.connsMu.Lock()
	_, exists := s.seen[key]
	if !exists {
		s.seen[key] = struct{}{}
	}
	s.connsMu.Unlock()

	conn := newUDPConn(c, s.opts, &s.stats)
	if !exists {
		s.stats.OnConnect()
		if s.handler != nil {
			s.handler.OnConnect(conn)
		}
	}
	return conn
}

func (s *Server) closeAll(err error) {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()
	if s.handler == nil {
		s.seen = make(map[string]struct{})
		return
	}
	for addr := range s.seen {
		s.stats.OnClose()
		conn := &udpConn{
			id:         kknet.NextConnID(),
			remoteAddr: addr,
			opts:       s.opts,
			ctx:        context.Background(),
		}
		s.handler.OnClose(conn, err)
	}
	s.seen = make(map[string]struct{})
}

type udpConn struct {
	id         int64
	conn       gnet.Conn
	remoteAddr string
	opts       kknet.Options
	active     atomic.Bool
	stats      *kknet.Stats

	ctxMu sync.RWMutex
	ctx   context.Context
}

func newUDPConn(c gnet.Conn, opts kknet.Options, stats *kknet.Stats) *udpConn {
	conn := &udpConn{
		id:         kknet.NextConnID(),
		conn:       c,
		remoteAddr: c.RemoteAddr().String(),
		opts:       opts,
		stats:      stats,
		ctx:        context.Background(),
	}
	conn.active.Store(true)
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

func releaseBuffer(data buffers.IBuffer) {
	if b, ok := data.(*kkbuffer.ByteBuffer); ok {
		kkbuffer.Put(b)
	}
}
