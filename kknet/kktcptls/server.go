package kktcptls

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// Server represents a TCP/TLS server based on standard library net.
type Server struct {
	addr    string
	handler kknet.IConnLifecycleHandler
	opts    kknet.Options
	connMgr *kknet.ConnManager[*tlsConn]

	listener net.Listener
	started  atomic.Bool
	done     chan error

	stats  kknet.Stats
	connWg sync.WaitGroup
}

var _ kknet.IServer = (*Server)(nil)

// NewServer creates a new TCP/TLS server.
// When opts.TLSConfig is set, the server uses TLS; otherwise plain TCP.
func NewServer(addr string, handler kknet.IConnLifecycleHandler, opts kknet.Options) *Server {
	kknet.CheckOptions(&opts)
	return &Server{
		addr:    addr,
		handler: handler,
		opts:    opts,
		connMgr: kknet.NewConnManager[*tlsConn](),
	}
}

func (s *Server) Start() error {
	if s.started.Swap(true) {
		return nil
	}

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		s.started.Store(false)
		return err
	}

	if s.opts.TLSConfig != nil {
		ln = tls.NewListener(ln, s.opts.TLSConfig)
		s.opts.Logger.Infof("kktcptls tls server listen on %s", s.addr)
	} else {
		s.opts.Logger.Infof("kktcptls server listen on %s", s.addr)
	}

	s.listener = ln
	s.done = make(chan error, 1)
	go s.acceptLoop()
	return nil
}

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

	s.opts.Logger.Infof("kktcptls server shutdown, waiting for %v", timeout)

	// Stop accepting new connections
	if s.listener != nil {
		_ = s.listener.Close()
	}

	// Wait for accept loop to exit
	if s.done != nil {
		select {
		case <-s.done:
		case <-ctx.Done():
			s.opts.Logger.Warnf("kktcptls server accept loop shutdown timeout")
		}
	}

	// Close all active connections
	s.closeAllConnections(ctx)
	return nil
}

func (s *Server) acceptLoop() {
	defer func() { s.done <- nil }()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if !s.started.Load() {
				return
			}
			s.stats.AddError()
			s.opts.Logger.Errorf("kktcptls accept error: %v", err)
			continue
		}

		setTCPKeepAlive(conn)

		c := newTLSConn(conn, &s.opts, &s.stats)
		s.connMgr.AddConn(c)
		s.connWg.Add(1)

		s.stats.OnConnect()
		if s.handler != nil {
			kknet.SafeHandlerCall(s.opts.Logger, &s.stats, "kktcptls OnConnect", func() {
				s.handler.OnConnect(c)
			})
		}
		s.opts.Logger.Debugf("kktcptls server OnConnect: connId=%d, count=%d", c.id, s.connMgr.GetCount())

		go func() {
			defer s.connWg.Done()
			err := c.readLoop()
			c.closeWithError(s.handler, err)
			s.connMgr.RemoveConn(c.id)
			s.opts.Logger.Debugf("kktcptls server OnClose: connId=%d, count=%d", c.id, s.connMgr.GetCount())
		}()
	}
}

func (s *Server) closeAllConnections(ctx context.Context) {
	conns := make([]*tlsConn, 0, s.connMgr.GetCount())
	s.connMgr.RangeAllConns(func(id kknet.CONN_ID, conn kknet.IConn) bool {
		conns = append(conns, conn.(*tlsConn))
		return true
	})

	for _, c := range conns {
		select {
		case <-ctx.Done():
			return
		default:
			c.closeWithError(s.handler, kkerrors.ErrServerStopped)
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
		s.opts.Logger.Warnf("kktcptls server: some connections did not close within timeout")
	}
}

func (s *Server) Addr() string {
	return s.addr
}

func (s *Server) Stats() kknet.StatsSnapshot {
	return s.stats.Snapshot()
}

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
