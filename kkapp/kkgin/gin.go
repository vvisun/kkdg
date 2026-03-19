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

type GinComponent struct {
	*gin.Engine
	opt Options

	srv *http.Server
	ln  net.Listener

	startOnce sync.Once
	stopOnce  sync.Once
	serveErr  chan error
}

func NewGinComponent(opt Options) *GinComponent {
	engine := gin.New()
	engine.Use(gin.Recovery())
	cfgCors(engine, opt.CorsConfig)
	for _, mw := range opt.Middlewares {
		engine.Use(mw)
	}
	return &GinComponent{
		Engine: engine,
		opt:    opt,
	}
}

func (slf *GinComponent) Start() error {
	if slf.opt.RegisterRoutes != nil {
		slf.opt.RegisterRoutes(slf.Engine)
	}

	// Ensure bind failure is surfaced from OnStart quickly.
	ln, err := net.Listen("tcp", slf.opt.HttpAddr)
	if err != nil {
		return err
	}
	slf.ln = ln

	slf.srv = &http.Server{
		Addr:    slf.opt.HttpAddr,
		Handler: slf.Engine,
	}

	slf.serveErr = make(chan error, 1)
	slf.startOnce.Do(func() {
		go func() {
			var serveErr error
			if slf.opt.CertFile != "" && slf.opt.KeyFile != "" {
				serveErr = slf.srv.ServeTLS(slf.ln, slf.opt.CertFile, slf.opt.KeyFile)
			} else {
				serveErr = slf.srv.Serve(slf.ln)
			}

			// Serve returns http.ErrServerClosed on graceful shutdown.
			if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
				slf.serveErr <- serveErr
				return
			}
			slf.serveErr <- nil
		}()
	})

	// Give Serve a tiny window to fail fast (e.g. TLS/cert errors).
	select {
	case err := <-slf.serveErr:
		if err != nil {
			_ = slf.srv.Close()
			return err
		}
		return nil
	case <-time.After(50 * time.Millisecond):
		return nil
	}
}

func (slf *GinComponent) Stop() error {
	if slf.srv == nil {
		return nil
	}

	timeout := slf.opt.ShutdownTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	slf.stopOnce.Do(func() {
		// Gracefully stop accepting new requests and wait in-flight ones.
		_ = slf.srv.Shutdown(ctx)
	})

	// Wait for Serve to exit (best-effort).
	select {
	case <-time.After(timeout):
		return nil
	case err := <-slf.serveErr:
		return err
	}
}
