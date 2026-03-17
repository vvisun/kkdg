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
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// Server represents a WebSocket server.
type Server struct {
	addr    string
	path    string
	handler kknet.IConnLifecycleHandler
	opts    kknet.Options

	httpServer *http.Server
	started    atomic.Bool
	booted     chan struct{}
	done       chan error

	stats kknet.Stats

	// Connection tracking for graceful shutdown
	connMgr *kknet.ConnManager[*wsConn]
	connWg  sync.WaitGroup
}

var _ kknet.IServer = (*Server)(nil)

// NewServer creates a new WebSocket server.
func NewServer(addr string, handler kknet.IConnLifecycleHandler, opts kknet.Options) *Server {
	kknet.CheckOptions(&opts)
	return &Server{
		addr:    addr,
		path:    "/ws",
		handler: handler,
		opts:    opts,
		connMgr: kknet.NewConnManager[*wsConn](),
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

	upgrader := websocket.Upgrader{
		ReadBufferSize:  s.opts.ReadBufferSize,
		WriteBufferSize: s.opts.WriteBufferSize,
		CheckOrigin:     s.opts.WsOriginChecker,
	}

	mux := http.NewServeMux()
	mux.HandleFunc(s.path, func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			s.stats.AddError()
			s.opts.Logger.Errorf("kkws upgrade error: %v", err)
			return
		}

		wsConn := newWSConn(conn, &s.opts, &s.stats)
		wsConn.conn.SetReadLimit(int64(s.opts.StreamTool.MaxPacketSize()))

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
		s.connMgr.AddConn(wsConn)
		s.connWg.Add(1)

		s.stats.OnConnect()
		if s.handler != nil {
			kknet.SafeHandlerCall(s.opts.Logger, &s.stats, "kkws OnConnect", func() {
				s.handler.OnConnect(wsConn)
			})
		}
		// s.opts.Logger.Debugf("kkws server OnConnect: connId=%d, count=%d", wsConn.id, s.connMgr.GetCount())

		go func() {
			defer s.connWg.Done()
			err := wsConn.readLoop()
			wsConn.closeWithError(s.handler, err)
			s.connMgr.RemoveConn(wsConn.id)
			// s.opts.Logger.Debugf("kkws server OnClose: connId=%d, count=%d", wsConn.id, s.connMgr.GetCount())
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
			ln, err := net.Listen("tcp", s.addr)
			if err != nil {
				s.stats.AddError()
				s.started.Store(false)
				s.done <- err
				return
			}
			s.opts.Logger.Infof("kkws server listen on %s%s", s.addr, s.path)
			close(s.booted)
			err = s.httpServer.Serve(ln)
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
		return err
	}
}

// Stop shuts down the server.
func (s *Server) Stop() error {
	if !s.started.Swap(false) {
		return kkerrors.ErrNetServerNotStarted
	}

	// Use timeout context for shutdown
	timeout := s.opts.ShutdownTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	s.opts.Logger.Infof("kkws server shutdown, waiting for %v", timeout)

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

	s.opts.Logger.Infof("kkws server shutdown... done")

	return nil
}

// closeAllConnections closes all active connections with context timeout.
func (s *Server) closeAllConnections(ctx context.Context) {
	s.opts.Logger.Infof("kkws server shutdown... closing all connections... count=%d", s.connMgr.GetCount())
	conns := make([]*wsConn, 0, s.connMgr.GetCount())
	s.connMgr.RangeAllConns(func(id kknet.CONN_ID, conn kknet.IConn) bool {
		conns = append(conns, conn.(*wsConn))
		return true
	})

	// Close all connections
	for _, conn := range conns {
		select {
		case <-ctx.Done():
			return
		default:
			conn.closeWithError(s.handler, kkerrors.ErrNetServerStopped)
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
	s.opts.Logger.Infof("kkws server shutdown... closed all connections done")
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

func (s *Server) SendMsg(connId kknet.CONN_ID, msg any) error {
	conn := s.connMgr.GetConn(connId)
	if conn == nil {
		return kkerrors.ErrNetConnNotFound
	}
	return conn.SendMsg(msg)
}

func (s *Server) SendBuffer(connId kknet.CONN_ID, buffer *kkbuffer.ByteBuffer) error {
	conn := s.connMgr.GetConn(connId)
	if conn == nil {
		kkbuffer.Put(buffer)
		return kkerrors.ErrNetConnNotFound
	}
	return conn.SendBuffer(buffer)
}
