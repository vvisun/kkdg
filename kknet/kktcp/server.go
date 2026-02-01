package kktcp

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/panjf2000/gnet/v2"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
)

// Server represents a TCP server with length-prefixed messages.
type Server struct {
	addr    string
	handler kknet.IHandler
	opts    kknet.Options
	connMgr *serverConnMgr

	engine  gnet.Engine
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
		connMgr: newServerConnMgr(),
	}
}

// Start begins listening and accepting connections.
func (s *Server) Start() error {
	if s.started.Swap(true) {
		return nil
	}

	// Apply middlewares to handler
	s.handler = kknet.ApplyMiddlewares(s.handler, s.opts.Middlewares...)

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
