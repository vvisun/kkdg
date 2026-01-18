package kknet

import (
	"crypto/tls"
	"net/http"
	"time"

	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/xos"
)

type OriginCheckFunc func(r *http.Request) bool

// Options are common network settings.
type Options struct {
	Logger          kklog.ILogger
	MaxMessageSize  int
	PoolSize        int
	ReadBufferSize  int
	WriteBufferSize int
	TLSConfig       *tls.Config
	OriginChecker   OriginCheckFunc
	ShutdownTimeout time.Duration
}

const (
	defaultMaxMessageSize  = 8 * 1024         //默认MaxMessageSize为8KB
	defaultBufferSize      = 64 * 1024        //默认缓冲区大小为64KB
	message_size_limit     = 1 * 1024 * 1024  //最大的MaxMessageSize不能超过该值: 1MB
	defaultShutdownTimeout = 30 * time.Second //默认关闭超时时间为30秒
)

func defaultOriginChecker(r *http.Request) bool {
	return true
}

// Option applies changes to Options.
type Option func(*Options)

// DefaultOptions returns default settings.
func DefaultOptions() Options {
	return Options{
		Logger:          kklog.Stdout(),
		MaxMessageSize:  defaultMaxMessageSize,
		PoolSize:        xos.NumCPU(),
		ReadBufferSize:  defaultBufferSize,
		WriteBufferSize: defaultBufferSize,
		TLSConfig:       nil,
		OriginChecker:   defaultOriginChecker,
		ShutdownTimeout: defaultShutdownTimeout,
	}
}

// ApplyOptions returns a configured Options.
func ApplyOptions(opts ...Option) Options {
	cfg := DefaultOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}

// WithLogger sets logger.
func WithLogger(l kklog.ILogger) Option {
	return func(o *Options) {
		if l != nil {
			o.Logger = l
		}
	}
}

// WithMaxMessageSize sets maximum allowed message size.
func WithMaxMessageSize(size int) Option {
	return func(o *Options) {
		if size > message_size_limit {
			size = message_size_limit
			kklog.Errorf("MaxMessageSize is too large, set to %d", message_size_limit)
		}
		if size > 0 {
			o.MaxMessageSize = size
		}
	}
}

// WithPoolSize enables ants pool with size.
func WithPoolSize(size int) Option {
	return func(o *Options) {
		if size > 0 {
			o.PoolSize = size
		}
	}
}

// WithBufferSizes sets read/write buffer sizes.
func WithBufferSizes(readSize, writeSize int) Option {
	return func(o *Options) {
		if readSize > 0 {
			o.ReadBufferSize = readSize
		}
		if writeSize > 0 {
			o.WriteBufferSize = writeSize
		}
	}
}

// WithOriginChecker sets origin checker.
func WithOriginChecker(checker OriginCheckFunc) Option {
	return func(o *Options) {
		if checker != nil {
			o.OriginChecker = checker
		}
	}
}

// WithTLSConfig enables TLS for supported protocols.
func WithTLSConfig(cfg *tls.Config) Option {
	return func(o *Options) {
		o.TLSConfig = cfg
	}
}

// WithShutdownTimeout sets the timeout for graceful shutdown.
func WithShutdownTimeout(timeout time.Duration) Option {
	return func(o *Options) {
		if timeout > 0 {
			o.ShutdownTimeout = timeout
		}
	}
}
