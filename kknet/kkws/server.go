package kkws

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/gorilla/websocket"
	"github.com/panjf2000/ants/v2"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
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
		CheckOrigin: func(r *http.Request) bool {
			_ = r
			return true
		},
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

	s.opts.Logger.Infof("kkws server listen on %s%s", s.addr, s.path)
	if s.opts.TLSConfig != nil {
		ln, err := net.Listen("tcp", s.addr)
		if err != nil {
			s.stats.AddError()
			return err
		}
		tlsListener := tls.NewListener(ln, s.opts.TLSConfig)
		err = s.httpServer.Serve(tlsListener)
		if err != nil && err != http.ErrServerClosed {
			s.stats.AddError()
			return err
		}
		return nil
	}
	err := s.httpServer.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		s.stats.AddError()
		return err
	}
	return nil
}

// Stop shuts down the server.
func (s *Server) Stop() error {
	if !s.started.Swap(false) {
		return kkerrors.ErrServerNotStarted
	}
	if s.httpServer != nil {
		_ = s.httpServer.Shutdown(context.Background())
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

func (s *Server) dispatch(c kknet.Conn, data []byte) {
	if s.handler == nil {
		return
	}
	if s.pool == nil {
		s.handler.OnMessage(c, data)
		return
	}
	if err := s.pool.Submit(func() {
		s.handler.OnMessage(c, data)
	}); err != nil {
		s.stats.AddError()
		s.opts.Logger.Errorf("kkws submit task error: %v", err)
	}
}

type wsConn struct {
	id    int64
	conn  *websocket.Conn
	opts  kknet.Options
	stats *kknet.Stats

	writeMu   sync.Mutex
	closeOnce sync.Once

	ctxMu sync.RWMutex
	ctx   context.Context
}

func newWSConn(conn *websocket.Conn, opts kknet.Options, stats *kknet.Stats) *wsConn {
	return &wsConn{
		id:    kknet.NextConnID(),
		conn:  conn,
		opts:  opts,
		stats: stats,
		ctx:   context.Background(),
	}
}

func (c *wsConn) ID() int64 {
	return c.id
}

func (c *wsConn) RemoteAddr() string {
	if c.conn == nil || c.conn.UnderlyingConn() == nil {
		return ""
	}
	return c.conn.UnderlyingConn().RemoteAddr().String()
}

func (c *wsConn) Send(data []byte) error {
	if len(data) > c.opts.MaxMessageSize {
		if c.stats != nil {
			c.stats.AddError()
		}
		return kkerrors.ErrMaxMessageSize
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if err := c.conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
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

func (c *wsConn) Close() error {
	c.closeWithError(nil, nil)
	return nil
}

func (c *wsConn) Context() context.Context {
	c.ctxMu.RLock()
	defer c.ctxMu.RUnlock()
	return c.ctx
}

func (c *wsConn) SetContext(ctx context.Context) {
	c.ctxMu.Lock()
	c.ctx = ctx
	c.ctxMu.Unlock()
}

func (c *wsConn) readLoop(dispatch func(kknet.Conn, []byte)) error {
	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			return err
		}
		if c.stats != nil {
			c.stats.AddRecv(len(data))
		}
		if dispatch != nil {
			dispatch(c, data)
		}
	}
}

func (c *wsConn) closeWithError(handler kknet.Handler, err error) {
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
