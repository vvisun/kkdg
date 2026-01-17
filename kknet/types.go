package kknet

import (
	"context"
	"crypto/tls"
	"net/http"
	"sync/atomic"

	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/kklog"
)

// Conn represents a network connection.
type Conn interface {
	ID() int64
	Send(data []byte) error
	Close() error
	RemoteAddr() string
	Context() context.Context
	SetContext(ctx context.Context)
}

// Handler handles connection lifecycle and messages.
type Handler interface {
	OnConnect(c Conn)
	OnMessage(c Conn, data buffers.IBuffer)
	OnClose(c Conn, err error)
}

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
}

const (
	defaultMaxMessageSize = 4 * 1024 * 1024
	defaultBufferSize     = 64 * 1024
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
		PoolSize:        0,
		ReadBufferSize:  defaultBufferSize,
		WriteBufferSize: defaultBufferSize,
		TLSConfig:       nil,
		OriginChecker:   defaultOriginChecker,
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

var connIDCounter atomic.Int64

// NextConnID returns a unique connection id.
func NextConnID() int64 {
	return connIDCounter.Add(1)
}
