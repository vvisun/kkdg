package kkgws

import (
	"bufio"
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lxzan/gws"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// Server represents a WebSocket server backed by gws.
type Server struct {
	addr    string
	path    string
	handler kknet.IConnLifecycleHandler
	opts    kknet.Options

	upgrader *gws.Upgrader
	listener net.Listener
	started  atomic.Bool
	booted   chan struct{}
	done     chan struct{}

	stats kknet.Stats

	connMgr *kknet.ConnManager[*gwsConn]
	connWg  sync.WaitGroup
}

var _ kknet.IServer = (*Server)(nil)

// NewServer creates a new gws-based WebSocket server.
func NewServer(addr string, handler kknet.IConnLifecycleHandler, opts kknet.Options) *Server {
	kknet.CheckOptions(&opts)
	return &Server{
		addr:    addr,
		path:    "/ws",
		handler: handler,
		opts:    opts,
		connMgr: kknet.NewConnManager[*gwsConn](),
	}
}

// SetPath sets the websocket upgrade path, call before Start.
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

	s.upgrader = gws.NewUpgrader(&gwsEventHandler{server: s}, &gws.ServerOption{
		ReadBufferSize:  s.opts.ReadBufferSize,
		WriteBufferSize: s.opts.WriteBufferSize,
		ParallelEnabled: false,
	})

	var ln net.Listener
	var err error
	if s.opts.TLSConfig != nil {
		s.opts.Logger.Infof("kkgws tls server start... listening on %s", s.addr)
		ln, err = tls.Listen("tcp", s.addr, s.opts.TLSConfig)
	} else {
		s.opts.Logger.Infof("kkgws server start... listening on %s", s.addr)
		ln, err = net.Listen("tcp", s.addr)
	}
	if err != nil {
		s.started.Store(false)
		s.opts.Logger.Infof("kkgws server start... failed: %v", err)
		return err
	}
	s.listener = ln

	s.booted = make(chan struct{})
	s.done = make(chan struct{})

	proto := "ws"
	if s.opts.TLSConfig != nil {
		proto = "wss"
	}
	s.opts.Logger.Infof("kkgws server start... listening on %s%s (%s)", s.addr, s.path, proto)
	close(s.booted)

	go s.acceptLoop()

	return nil
}

func (s *Server) acceptLoop() {
	defer close(s.done)
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if !s.started.Load() {
				return
			}
			s.stats.AddError()
			s.opts.Logger.Errorf("kkgws accept error: %v", err)
			continue
		}
		s.connWg.Add(1)
		go func(nc net.Conn) {
			defer s.connWg.Done()
			s.handleRawConn(nc)
		}(conn)
	}
}

func (s *Server) handleRawConn(conn net.Conn) {
	br := bufio.NewReaderSize(conn, s.opts.ReadBufferSize)
	r, err := http.ReadRequest(br)
	if err != nil {
		s.stats.AddError()
		conn.Close()
		return
	}
	if r.URL.Path != s.path {
		writeHTTP404(conn)
		conn.Close()
		return
	}
	socket, err := s.upgrader.UpgradeFromConn(conn, br, r)
	if err != nil {
		s.stats.AddError()
		s.opts.Logger.Errorf("kkgws upgrade error: %v", err)
		conn.Close()
		return
	}
	socket.SetNoDelay(true)
	socket.ReadLoop()
}

// Stop shuts down the server.
func (s *Server) Stop() error {
	if !s.started.Swap(false) {
		return kkerrors.ErrNetServerNotStarted
	}

	timeout := s.opts.ShutdownTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	s.opts.Logger.Infof("kkgws server shutdown, waiting for %v", timeout)

	if s.listener != nil {
		_ = s.listener.Close()
	}

	s.closeAllConnections(ctx)

	select {
	case <-s.done:
	case <-ctx.Done():
		s.opts.Logger.Warnf("kkgws server shutdown timeout after %v", timeout)
	}

	s.opts.Logger.Infof("kkgws server shutdown... done")

	return nil
}

func (s *Server) closeAllConnections(ctx context.Context) {
	s.opts.Logger.Infof("kkgws server shutdown... closing all connections... count=%d", s.connMgr.GetCount())
	conns := make([]*gwsConn, 0, s.connMgr.GetCount())
	s.connMgr.RangeAllConns(func(id kknet.CONN_ID, conn kknet.IConn) bool {
		conns = append(conns, conn.(*gwsConn))
		return true
	})

	for _, c := range conns {
		select {
		case <-ctx.Done():
			return
		default:
			_ = c.Close()
		}
	}

	done := make(chan struct{})
	go func() {
		s.connWg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		s.opts.Logger.Warnf("kkgws server: some connections did not close within timeout")
	}
	s.opts.Logger.Infof("kkgws server shutdown... closed all connections done")
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

// --- gws event handler ---

type gwsEventHandler struct {
	gws.BuiltinEventHandler
	server *Server
}

func (h *gwsEventHandler) OnOpen(socket *gws.Conn) {
	_ = socket.SetNoDelay(true)

	s := h.server
	c := newGwsConn(socket, &s.opts, &s.stats)
	socket.Session().Store(sessionKeyConn, c)

	s.connMgr.AddConn(c)
	s.stats.OnConnect()

	if s.handler != nil {
		kknet.SafeHandlerCall(s.opts.Logger, &s.stats, "kkgws OnConnect", func() {
			s.handler.OnConnect(c)
		})
	}
	// s.opts.Logger.Debugf("kkgws OnConnect: connId=%d, count=%d", c.id, s.connMgr.GetCount())

	c.startPingByTimingWheel()

	if s.opts.ReadTimeout > 0 {
		_ = socket.SetReadDeadline(time.Now().Add(s.opts.ReadTimeout))
	}
}

func (h *gwsEventHandler) OnClose(socket *gws.Conn, err error) {
	c := getGwsConn(socket)
	if c == nil {
		return
	}
	s := h.server
	c.doClose(s.handler, err)
	s.connMgr.RemoveConn(c.id)
	// s.opts.Logger.Debugf("kkgws OnClose: connId=%d, count=%d", c.id, s.connMgr.GetCount())
}

func (h *gwsEventHandler) OnPing(socket *gws.Conn, payload []byte) {
	_ = socket.WritePong(payload)
	if h.server.opts.ReadTimeout > 0 {
		_ = socket.SetReadDeadline(time.Now().Add(h.server.opts.ReadTimeout))
	}
}

func (h *gwsEventHandler) OnPong(socket *gws.Conn, payload []byte) {
	if h.server.opts.ReadTimeout > 0 {
		_ = socket.SetReadDeadline(time.Now().Add(h.server.opts.ReadTimeout))
	}
}

func (h *gwsEventHandler) OnMessage(socket *gws.Conn, message *gws.Message) {
	defer message.Close()

	if message.Opcode != gws.OpcodeBinary {
		return
	}

	c := getGwsConn(socket)
	if c == nil {
		return
	}
	c.onRecvMessage(message.Bytes())
}

// --- helpers ---

func writeHTTP404(conn net.Conn) {
	const body = "404 Not Found"
	_, _ = conn.Write([]byte("HTTP/1.1 404 Not Found\r\nContent-Length: 13\r\nConnection: close\r\n\r\n" + body))
}
