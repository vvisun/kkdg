package kkudp

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/panjf2000/gnet/v2"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
)

// Server represents a UDP server.
type Server struct {
	addr    string
	handler kknet.IConnLifecycleHandler
	opts    kknet.Options
	connMgr *serverConnMgr

	engine  gnet.Engine
	started atomic.Bool
	booted  chan struct{}
	done    chan error

	conns   map[string]*udpConn
	connsMu sync.Mutex

	stats kknet.Stats

	cleanupStop chan struct{}
	cleanupDone chan struct{}
}

var _ kknet.IServer = (*Server)(nil)

// NewServer creates a new UDP server.
func NewServer(addr string, handler kknet.IConnLifecycleHandler, opts kknet.Options) *Server {
	kknet.CheckOptions(&opts)
	s := &Server{
		addr:    addr,
		handler: handler,
		opts:    opts,
		conns:   make(map[string]*udpConn),
		connMgr: newServerConnMgr(),
	}
	s.connMgr.server = s
	return s
}

// Start begins listening for datagrams.
func (s *Server) Start() error {
	if s.started.Swap(true) {
		return nil
	}

	// Apply middlewares to handler
	// s.handler = kknet.ApplyMiddlewares(s.handler, s.opts.Middlewares...)

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

// GetConnManager returns the connection manager.
func (s *Server) GetConnManager() kknet.IConnManager {
	return s.connMgr
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
		s.connMgr.addConn(conn)
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

func (s *Server) closeConn(conn *udpConn, err error) {
	s.stats.OnClose()
	s.connMgr.removeConn(conn.id)
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

func (s *Server) removeConnByID(id int64) *udpConn {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()
	for key, conn := range s.conns {
		if conn.id == id {
			delete(s.conns, key)
			return conn
		}
	}
	return nil
}
