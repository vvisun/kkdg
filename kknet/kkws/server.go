package kkws

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"sync/atomic"

	"github.com/gorilla/websocket"
	"github.com/panjf2000/ants/v2"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers"
)

// Server represents a WebSocket server.
type Server struct {
	addr    string
	path    string
	handler kknet.Handler
	opts    kknet.Options

	httpServer *http.Server
	pool       *ants.Pool
	started    atomic.Bool
	booted     chan struct{}
	done       chan error

	stats kknet.Stats
}

// NewServer creates a new WebSocket server.
func NewServer(addr string, handler kknet.Handler, opts ...kknet.Option) *Server {
	return &Server{
		addr:    addr,
		path:    "/ws",
		handler: handler,
		opts:    kknet.ApplyOptions(opts...),
	}
}

// SetPath sets the websocket path, call before Start.
func (s *Server) SetPath(path string) {
	if path == "" {
		return
	}
	s.path = path
}

// Start begins listening for websocket connections.
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

	upgrader := websocket.Upgrader{
		ReadBufferSize:  s.opts.ReadBufferSize,
		WriteBufferSize: s.opts.WriteBufferSize,
		CheckOrigin:     s.opts.OriginChecker,
	}

	mux := http.NewServeMux()
	mux.HandleFunc(s.path, func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			s.stats.AddError()
			s.opts.Logger.Errorf("kkws upgrade error: %v", err)
			return
		}

		wsConn := newWSConn(conn, s.opts, &s.stats)
		wsConn.conn.SetReadLimit(int64(s.opts.MaxMessageSize))

		s.stats.OnConnect()
		if s.handler != nil {
			s.handler.OnConnect(wsConn)
		}

		go func() {
			err := wsConn.readLoop(s.dispatch)
			wsConn.closeWithError(s.handler, err)
		}()
	})

	s.httpServer = &http.Server{
		Addr:    s.addr,
		Handler: mux,
	}

	s.booted = make(chan struct{})
	s.done = make(chan error, 1)

	if s.opts.TLSConfig != nil {
		go func() {
			ln, err := net.Listen("tcp", s.addr)
			if err != nil {
				s.stats.AddError()
				s.started.Store(false)
				s.done <- err
				return
			}
			s.opts.Logger.Infof("kkws tls server listen on %s%s", s.addr, s.path)
			close(s.booted)
			tlsListener := tls.NewListener(ln, s.opts.TLSConfig)
			err = s.httpServer.Serve(tlsListener)
			if err != nil && err != http.ErrServerClosed {
				s.stats.AddError()
			}
			s.done <- err
		}()
	} else {
		go func() {
			s.opts.Logger.Infof("kkws server listen on %s%s", s.addr, s.path)
			close(s.booted)
			err := s.httpServer.ListenAndServe()
			if err != nil && err != http.ErrServerClosed {
				s.stats.AddError()
			}
			s.done <- err
		}()
	}

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
	if s.httpServer != nil {
		_ = s.httpServer.Shutdown(context.Background())
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

// Addr returns the server address.
func (s *Server) Addr() string {
	return s.addr
}

// Stats returns a snapshot of server statistics.
func (s *Server) Stats() kknet.StatsSnapshot {
	return s.stats.Snapshot()
}

func (s *Server) dispatch(c kknet.Conn, data buffers.IBuffer) {
	if s.handler == nil {
		releaseBuffer(data)
		return
	}
	if s.pool == nil {
		s.handler.OnMessage(c, data)
		releaseBuffer(data)
		return
	}
	if err := s.pool.Submit(func() {
		s.handler.OnMessage(c, data)
		releaseBuffer(data)
	}); err != nil {
		releaseBuffer(data)
		s.stats.AddError()
		s.opts.Logger.Errorf("kkws submit task error: %v", err)
	}
}
