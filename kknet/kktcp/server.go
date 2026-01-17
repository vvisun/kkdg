package kktcp

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"sync/atomic"

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
	handler kknet.Handler
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

	unpacker kknet.StreamUnpacker
}

// NewServer creates a new TCP server.
func NewServer(addr string, handler kknet.Handler, opts ...kknet.Option) *Server {
	cfg := kknet.ApplyOptions(opts...)
	return &Server{
		addr:     addr,
		handler:  handler,
		opts:     cfg,
		unpacker: kknet.NewLengthFieldUnpacker(cfg.MaxMessageSize),
	}
}

// SetUnpacker overrides the default length-field unpacker.
// Call before Start.
func (s *Server) SetUnpacker(u kknet.StreamUnpacker) {
	if u == nil {
		return
	}
	s.unpacker = u
}

// Start begins listening and accepting connections.
func (s *Server) Start() error {
	if s.started.Swap(true) {
		return nil
	}

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
	if s.opts.TLSConfig != nil {
		return s.stopTLS()
	}
	if err := s.engine.Stop(context.Background()); err != nil {
		return err
	}
	if s.done != nil {
		_ = <-s.done
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

func (s *Server) stopTLS() error {
	if s.tlsListener != nil {
		_ = s.tlsListener.Close()
	}
	s.closeAllTLS()
	s.tlsWg.Wait()
	return nil
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
		s.handler.OnConnect(tc)
	}

	s.tlsWg.Add(1)
	go func() {
		defer s.tlsWg.Done()
		err := tc.readLoop(s.handler)
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
		h.server.handler.OnConnect(tconn)
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
		h.server.handler.OnClose(tc, err)
	}
	return gnet.None
}

func (h *tcpEventHandler) OnTraffic(c gnet.Conn) (action gnet.Action) {
	tc, ok := c.Context().(*tcpConn)
	if !ok {
		return gnet.Close
	}

	for {
		data, ok, err := h.server.unpacker.Unpack(c)
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
			payload.B = append(payload.B[:0], data...)
			h.dispatch(tc, payload)
		}
	}
}

func (h *tcpEventHandler) dispatch(c *tcpConn, data buffers.IBuffer) {
	if h.server.pool == nil {
		h.server.handler.OnMessage(c, data)
		releaseBuffer(data)
		return
	}
	if err := h.server.pool.Submit(func() {
		h.server.handler.OnMessage(c, data)
		releaseBuffer(data)
	}); err != nil {
		releaseBuffer(data)
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
	buf := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(data)))
	copy(buf[4:], data)
	err := c.conn.AsyncWrite(buf, nil)
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
	bb := kkbuffer.Get()
	bb.B = bb.B[:0]
	bb.B = append(bb.B, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(bb.B[:4], uint32(len(data)))
	bb.B = append(bb.B, data...)
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

func (c *tlsConn) readLoop(handler kknet.Handler) error {
	header := make([]byte, 4)
	for {
		if err := readFull(c.conn, header); err != nil {
			return err
		}
		size := int(binary.BigEndian.Uint32(header))
		if size < 0 || size > c.opts.MaxMessageSize {
			if c.stats != nil {
				c.stats.AddError()
			}
			return kkerrors.ErrMaxMessageSize
		}
		payload := kkbuffer.Get()
		if cap(payload.B) < size {
			payload.B = make([]byte, size)
		} else {
			payload.B = payload.B[:size]
		}
		if err := readFull(c.conn, payload.B); err != nil {
			kkbuffer.Put(payload)
			return err
		}
		if c.stats != nil {
			c.stats.AddRecv(len(payload.B))
		}
		if handler != nil {
			handler.OnMessage(c, payload)
		}
		kkbuffer.Put(payload)
	}
}

func (c *tlsConn) closeWithError(handler kknet.Handler, err error) {
	c.closeOnce.Do(func() {
		if c.stats != nil {
			c.stats.OnClose()
			if err != nil {
				c.stats.AddError()
			}
		}
		_ = c.conn.Close()
		if handler != nil {
			handler.OnClose(c, err)
		}
	})
}

func releaseBuffer(data buffers.IBuffer) {
	if b, ok := data.(*kkbuffer.ByteBuffer); ok {
		kkbuffer.Put(b)
	}
}
