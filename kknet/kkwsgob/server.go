package kkwsgob

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/panjf2000/gnet/v2"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// Server represents a WebSocket server based on gnet + gobwas/ws.
type Server struct {
	addr    string
	path    string
	handler kknet.IHandler
	opts    kknet.Options
	connMgr *serverConnMgr

	engine  gnet.Engine
	pool    *ants.Pool
	started atomic.Bool
	booted  chan struct{}
	done    chan error

	stats kknet.Stats
}

var _ kknet.IServer = (*Server)(nil)

// NewServer creates a new WebSocket server.
func NewServer(addr string, handler kknet.IHandler, opts ...kknet.Option) *Server {
	return &Server{
		addr:    addr,
		path:    "/ws",
		handler: handler,
		opts:    kknet.ApplyOptions(opts...),
		connMgr: newServerConnMgr(),
	}
}

// SetPath sets the websocket path, call before Start.
func (s *Server) SetPath(path string) {
	if path == "" {
		return
	}
	s.path = path
}

// Start begins listening and accepting connections.
func (s *Server) Start() error {
	if s.started.Swap(true) {
		return nil
	}

	if s.opts.TLSConfig != nil {
		s.started.Store(false)
		return errors.New("kkwsgob server does not support TLS")
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

	handler := &wsEventHandler{server: s}
	go func() {
		err := gnet.Run(handler, "tcp://"+s.addr,
			gnet.WithMulticore(true),
			gnet.WithReadBufferCap(s.opts.ReadBufferSize),
			gnet.WithWriteBufferCap(s.opts.WriteBufferSize),
		)
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

	timeout := s.opts.ShutdownTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := s.engine.Stop(ctx); err != nil {
		if ctx.Err() != nil {
			s.opts.Logger.Warnf("kkwsgob server shutdown timeout after %v", timeout)
		}
		return err
	}

	if s.done != nil {
		select {
		case <-s.done:
		case <-ctx.Done():
			s.opts.Logger.Warnf("kkwsgob server shutdown timeout after %v", timeout)
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

func (s *Server) dispatch(c *wsConn, data buffers.IBuffer) {
	if s.handler == nil {
		kkbuffer.Put(data)
		return
	}
	if s.pool == nil {
		defer kkbuffer.Put(data)
		kknet.SafeHandlerCall(s.opts.Logger, &s.stats, "kkwsgob OnMessage", func() {
			s.handler.OnMessage(c, data)
		})
		return
	}
	if err := s.pool.Submit(func() {
		defer kkbuffer.Put(data)
		kknet.SafeHandlerCall(s.opts.Logger, &s.stats, "kkwsgob OnMessage", func() {
			s.handler.OnMessage(c, data)
		})
	}); err != nil {
		kkbuffer.Put(data)
		s.stats.AddError()
		s.opts.Logger.Errorf("kkwsgob submit task error: %v", err)
	}
}
