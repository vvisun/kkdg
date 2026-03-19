package kkgin

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// Server 封装 Gin Engine 与 http.Server，作为普通 HTTP 模块使用（与 kkapp 组件生命周期解耦）。
type Server struct {
	*gin.Engine
	opt Options

	srv *http.Server
	ln  net.Listener

	startOnce sync.Once
	stopOnce  sync.Once
	serveErr  chan error
}

// NewServer 创建 HTTP 服务：应用 CORS、中间件，并在此时调用 RegisterRoutes（若已配置）。
func NewServer(opt Options) *Server {
	engine := gin.New()
	engine.Use(gin.Recovery())
	cfgCors(engine, opt.CorsConfig)
	for _, mw := range opt.Middlewares {
		engine.Use(mw)
	}
	if opt.RegisterRoutes != nil {
		opt.RegisterRoutes(engine)
	}
	return &Server{
		Engine: engine,
		opt:    opt,
	}
}

// Start 监听 HttpAddr 并在后台提供 HTTP/HTTPS 服务。
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.opt.HttpAddr)
	if err != nil {
		return err
	}
	s.ln = ln

	s.srv = &http.Server{
		Addr:    s.opt.HttpAddr,
		Handler: s.Engine,
	}

	s.serveErr = make(chan error, 1)
	s.startOnce.Do(func() {
		go func() {
			var serveErr error
			if s.opt.CertFile != "" && s.opt.KeyFile != "" {
				serveErr = s.srv.ServeTLS(s.ln, s.opt.CertFile, s.opt.KeyFile)
			} else {
				serveErr = s.srv.Serve(s.ln)
			}

			if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
				s.serveErr <- serveErr
				return
			}
			s.serveErr <- nil
		}()
	})

	select {
	case err := <-s.serveErr:
		if err != nil {
			_ = s.srv.Close()
			return err
		}
		return nil
	case <-time.After(50 * time.Millisecond):
		return nil
	}
}

// Stop 优雅关闭，等待 Serve 退出（最长 ShutdownTimeout）。
func (s *Server) Stop() error {
	if s.srv == nil {
		return nil
	}

	timeout := s.opt.ShutdownTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	s.stopOnce.Do(func() {
		_ = s.srv.Shutdown(ctx)
	})

	select {
	case <-time.After(timeout):
		return nil
	case err := <-s.serveErr:
		return err
	}
}
