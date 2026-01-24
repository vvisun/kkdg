package kkwsgob

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gobwas/ws"
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

	tlsListener net.Listener
	tlsWg       sync.WaitGroup

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
		return s.startTLSServer()
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

func (s *Server) startTLSServer() error {
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

	ln, err := tls.Listen("tcp", s.addr, s.opts.TLSConfig)
	if err != nil {
		s.started.Store(false)
		return err
	}
	s.tlsListener = ln
	s.booted = make(chan struct{})
	s.done = make(chan error, 1)

	go func() {
		s.opts.Logger.Infof("kkwsgob tls server listen on %s%s", s.addr, s.path)
		close(s.booted)
		err := s.acceptTLS(ln)
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

func (s *Server) acceptTLS(ln net.Listener) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				s.stats.AddError()
				continue
			}
			s.stats.AddError()
			return err
		}
		s.tlsWg.Add(1)
		go s.handleTLSConn(conn)
	}
}

func (s *Server) handleTLSConn(conn net.Conn) {
	defer s.tlsWg.Done()

	wc := newNetWSConn(conn, nil, s.opts, &s.stats, ws.StateServerSide)
	if err := s.upgradeTLS(conn); err != nil {
		wc.setCloseErr(err)
		_ = conn.Close()
		return
	}

	wc.upgraded = true
	s.connMgr.addConn(wc)
	s.stats.OnConnect()
	if s.handler != nil {
		kknet.SafeHandlerCall(s.opts.Logger, &s.stats, "kkwsgob OnConnect", func() {
			s.handler.OnConnect(wc)
		})
	}

	err := wc.readLoop(func(msg buffers.IBuffer) {
		s.dispatch(wc, msg)
	})
	if err != nil && !isEOF(err) {
		sendCloseOnErrorNet(wc, err)
	}
	wc.setCloseErr(err)
	s.closeTLSConn(wc, err)
}

func (s *Server) upgradeTLS(conn net.Conn) error {
	var (
		reqURI  []byte
		reqHost string
		headers = make(http.Header)
	)

	upgrader := ws.Upgrader{
		ReadBufferSize:  s.opts.ReadBufferSize,
		WriteBufferSize: s.opts.WriteBufferSize,
		OnRequest: func(uri []byte) error {
			reqURI = append(reqURI[:0], uri...)
			if s.path == "" {
				return nil
			}
			u, err := url.ParseRequestURI(string(uri))
			if err != nil {
				return ws.RejectConnectionError(
					ws.RejectionStatus(http.StatusBadRequest),
					ws.RejectionReason("invalid request uri"),
				)
			}
			if u.Path != s.path {
				return ws.RejectConnectionError(
					ws.RejectionStatus(http.StatusNotFound),
					ws.RejectionReason("invalid websocket path"),
				)
			}
			return nil
		},
		OnHost: func(host []byte) error {
			reqHost = string(host)
			return nil
		},
		OnHeader: func(key, value []byte) error {
			headers.Add(string(key), string(value))
			return nil
		},
		OnBeforeUpgrade: func() (ws.HandshakeHeader, error) {
			if s.opts.WsOriginChecker == nil {
				return nil, nil
			}
			req := &http.Request{
				Method: http.MethodGet,
				Host:   reqHost,
				Header: headers,
			}
			if len(reqURI) > 0 {
				u, err := url.ParseRequestURI(string(reqURI))
				if err != nil {
					return nil, ws.RejectConnectionError(
						ws.RejectionStatus(http.StatusBadRequest),
						ws.RejectionReason("invalid request uri"),
					)
				}
				req.URL = u
				req.RequestURI = u.RequestURI()
			}
			if !s.opts.WsOriginChecker(req) {
				return nil, ws.RejectConnectionError(
					ws.RejectionStatus(http.StatusForbidden),
					ws.RejectionReason("forbidden origin"),
				)
			}
			return nil, nil
		},
	}

	_, err := upgrader.Upgrade(conn)
	return err
}

func (s *Server) closeTLSConn(wc *netWSConn, err error) {
	if wc.fragBuf != nil {
		kkbuffer.Put(wc.fragBuf)
		wc.fragBuf = nil
	}
	s.stats.OnClose()
	if err == nil {
		err = wc.getCloseErr()
	}
	if err != nil && !isExpectedCloseErr(err) {
		s.stats.AddError()
	}
	s.connMgr.removeConn(wc.id)
	if s.handler != nil {
		kknet.SafeHandlerCall(s.opts.Logger, &s.stats, "kkwsgob OnClose", func() {
			s.handler.OnClose(wc, err)
		})
	}
}

func sendCloseOnErrorNet(wc *netWSConn, err error) {
	code := ws.StatusProtocolError
	if errors.Is(err, kkerrors.ErrMaxMessageSize) {
		code = ws.StatusMessageTooBig
	}
	payload := ws.NewCloseFrameBody(code, "")
	wc.writeControl(ws.OpClose, payload)
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

	if s.opts.TLSConfig != nil {
		if s.tlsListener != nil {
			_ = s.tlsListener.Close()
		}
		done := make(chan struct{})
		go func() {
			s.tlsWg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-ctx.Done():
			s.opts.Logger.Warnf("kkwsgob server shutdown timeout after %v", timeout)
		}
	} else {
		if err := s.engine.Stop(ctx); err != nil {
			if ctx.Err() != nil {
				s.opts.Logger.Warnf("kkwsgob server shutdown timeout after %v", timeout)
			}
			return err
		}
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

func (s *Server) dispatch(c kknet.IConn, data buffers.IBuffer) {
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
