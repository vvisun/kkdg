package kktcp

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/panjf2000/ants/v2"
	"github.com/panjf2000/gnet/v2"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
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
}

// NewServer creates a new TCP server.
func NewServer(addr string, handler kknet.Handler, opts ...kknet.Option) *Server {
	return &Server{
		addr:    addr,
		handler: handler,
		opts:    kknet.ApplyOptions(opts...),
	}
}

// Start begins listening and accepting connections.
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
	tconn := newTCPConn(c, h.server.opts)
	c.SetContext(tconn)
	if h.server.handler != nil {
		h.server.handler.OnConnect(tconn)
	}
	return nil, gnet.None
}

func (h *tcpEventHandler) OnClose(c gnet.Conn, err error) (action gnet.Action) {
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
		if c.InboundBuffered() < 4 {
			return gnet.None
		}
		header, err := c.Peek(4)
		if err != nil {
			if errors.Is(err, io.ErrShortBuffer) {
				return gnet.None
			}
			return gnet.Close
		}
		size := int(binary.BigEndian.Uint32(header))
		if size < 0 || size > tc.opts.MaxMessageSize {
			return gnet.Close
		}
		if c.InboundBuffered() < 4+size {
			return gnet.None
		}
		_, _ = c.Discard(4)
		body, err := c.Next(size)
		if err != nil {
			return gnet.Close
		}
		if h.server.handler != nil {
			payload := make([]byte, len(body))
			copy(payload, body)
			h.dispatch(tc, payload)
		}
	}
}

func (h *tcpEventHandler) dispatch(c *tcpConn, data []byte) {
	if h.server.pool == nil {
		h.server.handler.OnMessage(c, data)
		return
	}
	if err := h.server.pool.Submit(func() {
		h.server.handler.OnMessage(c, data)
	}); err != nil {
		h.server.opts.Logger.Errorf("kktcp submit task error: %v", err)
	}
}

type tcpConn struct {
	id   int64
	conn gnet.Conn
	opts kknet.Options

	ctxMu sync.RWMutex
	ctx   context.Context
}

func newTCPConn(c gnet.Conn, opts kknet.Options) *tcpConn {
	return &tcpConn{
		id:   kknet.NextConnID(),
		conn: c,
		opts: opts,
		ctx:  context.Background(),
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
		return kkerrors.ErrMaxMessageSize
	}
	buf := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(data)))
	copy(buf[4:], data)
	return c.conn.AsyncWrite(buf, nil)
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
