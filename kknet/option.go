package kknet

import (
	"crypto/tls"
	"net/http"
	"time"

	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/xos"
)

type OriginCheckFunc func(r *http.Request) bool

// Options are common network settings.
type Options struct {
	Logger             kklog.ILogger          // 日志记录器
	MaxMessageSize     int                    // 最大消息大小
	PoolSize           int                    // ants池大小（注意：为0时，不使用ants池。建议使用，以提高性能。默认为CPU核心数）
	ReadBufferSize     int                    // 读缓冲区大小
	WriteBufferSize    int                    // 写缓冲区大小
	TLSConfig          *tls.Config            // TLS配置
	OriginChecker      OriginCheckFunc        // websocket原始检查器
	ShutdownTimeout    time.Duration          // 服务关闭超时时间
	UDPConnIdleTimeout time.Duration          // UDP连接空闲超时时间（为0时，不启用空闲清理）
	UDPCleanupInterval time.Duration          // UDP清理间隔时间（为0时，不启用清理）
	ReadTimeout        time.Duration          // WebSocket读超时时间（为0时，不启用读超时）
	WriteTimeout       time.Duration          // WebSocket写超时时间（为0时，不启用写超时）
	Middlewares        []Middleware           // 中间件列表
	StreamPacket       kkpacket.IStreamPacket //流处理器, used for tcp
}

const (
	defaultMaxMessageSize     = 8 * 1024         //默认MaxMessageSize为8KB
	defaultBufferSize         = 64 * 1024        //默认缓冲区大小为64KB
	message_size_limit        = 1 * 1024 * 1024  //最大的MaxMessageSize不能超过该值: 1MB
	defaultShutdownTimeout    = 30 * time.Second //默认关闭超时时间为30秒
	defaultUDPConnIdleTimeout = 5 * time.Minute  //默认UDP连接空闲超时时间为5分钟
	defaultUDPCleanupInterval = 1 * time.Minute  //默认UDP清理间隔时间为1分钟
	defaultReadTimeout        = 0                //默认WebSocket读超时为0（不超时）
	defaultWriteTimeout       = 0                //默认WebSocket写超时为0（不超时）
)

func defaultOriginChecker(r *http.Request) bool {
	return true
}

// Option applies changes to Options.
type Option func(*Options)

// DefaultOptions returns default settings.
func DefaultOptions() Options {
	return Options{
		Logger:             kklog.Stdout(),
		MaxMessageSize:     defaultMaxMessageSize,
		PoolSize:           xos.NumCPU(),
		ReadBufferSize:     defaultBufferSize,
		WriteBufferSize:    defaultBufferSize,
		TLSConfig:          nil,
		OriginChecker:      defaultOriginChecker,
		ShutdownTimeout:    defaultShutdownTimeout,
		UDPConnIdleTimeout: defaultUDPConnIdleTimeout,
		UDPCleanupInterval: defaultUDPCleanupInterval,
		ReadTimeout:        defaultReadTimeout,
		WriteTimeout:       defaultWriteTimeout,
		StreamPacket:       kkpacket.NewLengthFieldStreamPacket(nil),
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

// WithStreamPacket sets stream packet.
func WithStreamPacket(streamPacket kkpacket.IStreamPacket) Option {
	return func(o *Options) {
		if streamPacket != nil {
			o.StreamPacket = streamPacket
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

// WithUDPConnIdleTimeout sets UDP connection idle timeout.
// Set to 0 to disable idle cleanup.
func WithUDPConnIdleTimeout(timeout time.Duration) Option {
	return func(o *Options) {
		o.UDPConnIdleTimeout = timeout
	}
}

// WithUDPCleanupInterval sets UDP cleanup interval.
// Set to 0 to disable idle cleanup.
func WithUDPCleanupInterval(interval time.Duration) Option {
	return func(o *Options) {
		o.UDPCleanupInterval = interval
	}
}

// WithReadTimeout sets the read timeout for WebSocket connections.
// Set to 0 to disable read timeout.
func WithReadTimeout(timeout time.Duration) Option {
	return func(o *Options) {
		if timeout >= 0 {
			o.ReadTimeout = timeout
		}
	}
}

// WithWriteTimeout sets the write timeout for WebSocket connections.
// Set to 0 to disable write timeout.
func WithWriteTimeout(timeout time.Duration) Option {
	return func(o *Options) {
		if timeout >= 0 {
			o.WriteTimeout = timeout
		}
	}
}

// WithMiddlewares sets middlewares.
func WithMiddleware(mw Middleware) Option {
	if mw == nil {
		kklog.Errorf("Middleware is nil")
		return func(o *Options) {}
	}
	return func(o *Options) {
		if o.Middlewares == nil {
			o.Middlewares = make([]Middleware, 0)
		}
		o.Middlewares = append(o.Middlewares, mw)
	}
}
