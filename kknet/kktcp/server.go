package kktcp

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/panjf2000/gnet/v2"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/xos"
)

// Server represents a TCP server with length-prefixed messages.
type Server struct {
	addr    string
	handler kknet.IConnLifecycleHandler
	opts    kknet.Options
	connMgr *kknet.ConnManager[*tcpConn]

	engine  gnet.Engine
	started atomic.Bool
	booted  chan struct{}
	done    chan error

	stats kknet.Stats
}

var _ kknet.IServer = (*Server)(nil)

// NewServer creates a new TCP server.
func NewServer(addr string, handler kknet.IConnLifecycleHandler, opts kknet.Options) *Server {
	kknet.CheckOptions(&opts)
	if opts.TLSConfig != nil {
		// panic as gnet server does not support TLS
		panic("gnet server does not support TLS. use kktcptls instead.")
	}
	return &Server{
		addr:    addr,
		handler: handler,
		opts:    opts,
		connMgr: kknet.NewConnManager[*tcpConn](),
	}
}

// Start begins listening and accepting connections.
func (s *Server) Start() error {
	if s.started.Swap(true) {
		return nil
	}

	s.opts.Logger.Infof("kktcp server start... listening on %s", s.addr)

	s.booted = make(chan struct{})
	s.done = make(chan error, 1)

	handler := &tcpEventHandler{server: s}
	go func() {
		err := gnet.Run(handler, "tcp://"+s.addr,
			gnet.WithMulticore(true),
			gnet.WithNumEventLoop(xos.NumCPU()),
			gnet.WithLogger(gnetNopLogger),
			gnet.WithTCPKeepAlive(10*time.Second),
			gnet.WithReadBufferCap(s.opts.ReadBufferSize),
			gnet.WithWriteBufferCap(s.opts.WriteBufferSize),
			gnet.WithTCPNoDelay(gnet.TCPNoDelay),
		)
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

	s.opts.Logger.Infof("kktcp server shutdown... waiting for %v", timeout)

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

	s.opts.Logger.Infof("kktcp server shutdown... done")

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

func (s *Server) SendMsg(connId kknet.CONN_ID, msg any) error {
	conn := s.connMgr.GetConn(connId)
	if conn == nil {
		return kkerrors.ErrConnNotFound
	}
	return conn.SendMsg(msg)
}

func (s *Server) SendBuffer(connId kknet.CONN_ID, buffer *kkbuffer.ByteBuffer) error {
	conn := s.connMgr.GetConn(connId)
	if conn == nil {
		kkbuffer.Put(buffer)
		return kkerrors.ErrConnNotFound
	}
	return conn.SendBuffer(buffer)
}
