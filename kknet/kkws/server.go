package kkws

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/panjf2000/ants/v2"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// Server represents a WebSocket server.
type Server struct {
	addr    string
	path    string
	handler kknet.IHandler
	opts    kknet.Options

	httpServer  *http.Server
	pool        *ants.Pool
	started     atomic.Bool
	booted      chan struct{}
	done        chan error
	middlewares []kknet.Middleware

	stats kknet.Stats

	// Connection tracking for graceful shutdown
	connsMu sync.RWMutex
	conns   map[int64]*wsConn
	connWg  sync.WaitGroup
}

// NewServer creates a new WebSocket server.
func NewServer(addr string, handler kknet.IHandler, opts ...kknet.Option) *Server {
	return &Server{
		addr:    addr,
		path:    "/ws",
		handler: handler,
		opts:    kknet.ApplyOptions(opts...),
		conns:   make(map[int64]*wsConn),
	}
}

// SetPath sets the websocket path, call before Start.
func (s *Server) SetPath(path string) {
	if path == "" {
		return
	}
	s.path = path
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

// Start begins listening for websocket connections.
func (s *Server) Start() error {
	if s.started.Swap(true) {
		return nil
	}

	// Apply middlewares to handler
	s.handler = kknet.ApplyMiddlewares(s.handler, s.middlewares...)

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

		// Set read/write timeouts if configured
		if s.opts.ReadTimeout > 0 {
			if err := conn.SetReadDeadline(time.Now().Add(s.opts.ReadTimeout)); err != nil {
				s.stats.AddError()
				s.opts.Logger.Warnf("kkws set read deadline error: %v", err)
			}
		}
		if s.opts.WriteTimeout > 0 {
			if err := conn.SetWriteDeadline(time.Now().Add(s.opts.WriteTimeout)); err != nil {
				s.stats.AddError()
				s.opts.Logger.Warnf("kkws set write deadline error: %v", err)
			}
		}

		// Track connection
		s.connsMu.Lock()
		s.conns[wsConn.id] = wsConn
		s.connsMu.Unlock()
		s.connWg.Add(1)

		s.stats.OnConnect()
		if s.handler != nil {
			kknet.SafeHandlerCall(s.opts.Logger, &s.stats, "kkws OnConnect", func() {
				s.handler.OnConnect(wsConn)
			})
		}

		go func() {
			defer s.connWg.Done()
			err := wsConn.readLoop(s.dispatch)
			wsConn.closeWithError(s.handler, err)
			// Remove from tracking
			s.connsMu.Lock()
			delete(s.conns, wsConn.id)
			s.connsMu.Unlock()
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

	// Use timeout context for shutdown
	timeout := s.opts.ShutdownTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Stop accepting new connections
	if s.httpServer != nil {
		if err := s.httpServer.Shutdown(ctx); err != nil {
			s.opts.Logger.Warnf("kkws server shutdown error: %v", err)
		}
	}

	// Close all active connections with timeout
	s.closeAllConnections(ctx)

	// Wait for done channel or timeout
	if s.done != nil {
		select {
		case <-s.done:
		case <-ctx.Done():
			s.opts.Logger.Warnf("kkws server shutdown timeout after %v", timeout)
		}
	}

	if s.pool != nil {
		s.pool.Release()
		s.pool = nil
	}
	return nil
}

// closeAllConnections closes all active connections with context timeout.
func (s *Server) closeAllConnections(ctx context.Context) {
	s.connsMu.Lock()
	conns := make([]*wsConn, 0, len(s.conns))
	for _, conn := range s.conns {
		conns = append(conns, conn)
	}
	s.connsMu.Unlock()

	// Close all connections
	for _, conn := range conns {
		select {
		case <-ctx.Done():
			return
		default:
			conn.closeWithError(s.handler, kkerrors.ErrServerStopped)
		}
	}

	// Wait for connections to close or timeout
	done := make(chan struct{})
	go func() {
		s.connWg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		s.opts.Logger.Warnf("kkws server: some connections did not close within timeout")
	}
}

// Addr returns the server address.
func (s *Server) Addr() string {
	return s.addr
}

// Stats returns a snapshot of server statistics.
func (s *Server) Stats() kknet.StatsSnapshot {
	return s.stats.Snapshot()
}

func (s *Server) dispatch(c kknet.IConn, data buffers.IBuffer) {
	if s.handler == nil {
		kkbuffer.Put(data)
		return
	}
	if s.pool == nil {
		defer kkbuffer.Put(data)
		kknet.SafeHandlerCall(s.opts.Logger, &s.stats, "kkws OnMessage", func() {
			s.handler.OnMessage(c, data)
		})
		return
	}
	if err := s.pool.Submit(func() {
		defer kkbuffer.Put(data)
		kknet.SafeHandlerCall(s.opts.Logger, &s.stats, "kkws OnMessage", func() {
			s.handler.OnMessage(c, data)
		})
	}); err != nil {
		kkbuffer.Put(data)
		s.stats.AddError()
		s.opts.Logger.Errorf("kkws submit task error: %v", err)
	}
}
