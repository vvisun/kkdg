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
	Logger              kklog.ILogger                // 日志记录器
	PoolSize            int                          // ants池大小（注意：为0时，不使用ants池。建议使用，以提高性能。默认为CPU核心数）
	ReadBufferSize      int                          // 读缓冲区大小
	WriteBufferSize     int                          // 写缓冲区大小
	ShutdownTimeout     time.Duration                // 服务关闭超时时间
	Middlewares         []Middleware                 // 中间件列表
	IsNeedReconnect     bool                         // 是否需要重连
	ReconnectInterval   time.Duration                // 重连间隔
	ReconnectMaxRetries int                          // 重连最大次数(<=0为无限)
	ReconnectCallback   func(attempt int, err error) // 重连回调(成功时 err 为 nil)

	TLSConfig *tls.Config // TLS配置。use for wss or tcp with tls

	UDPConnIdleTimeout time.Duration // UDP连接空闲超时时间（为0时，不启用空闲清理）
	UDPCleanupInterval time.Duration // UDP清理间隔时间（为0时，不启用清理）

	WsOriginChecker OriginCheckFunc // websocket原始检查器
	WsReadTimeout   time.Duration   // WebSocket读超时时间（为0时，不启用读超时）
	WsWriteTimeout  time.Duration   // WebSocket写超时时间（为0时，不启用写超时）

	SendQueueSize                 int                                     // 异步发送队列大小
	SendQueueStrict               bool                                    // 异步发送队列是否严格容量控制
	SendQueueNeedFlushOver        bool                                    //关闭时是否需要等待 flush 完成
	SendQueueTimeoutFlushOver     time.Duration                           //关闭时等待 flush 完成的超时时间
	SendQueueFlushTimeoutCallback func(conn IConn, timeout time.Duration) //flush 超时回调
}

func defaultWSOriginChecker(r *http.Request) bool {
	return true
}

// Option applies changes to Options.
type Option func(*Options)

// DefaultOptions returns default settings.
func DefaultOptions() Options {
	return Options{
		Logger:          kklog.Nop(),
		PoolSize:        xos.NumCPU(), // 默认使用CPU核心数
		ReadBufferSize:  64 * 1024,    // 64KB
		WriteBufferSize: 64 * 1024,    // 64KB
		TLSConfig:       nil,
		ShutdownTimeout: 10 * time.Second, // 10秒
		Middlewares:     nil,

		WsOriginChecker: defaultWSOriginChecker,
		WsReadTimeout:   0, // 0秒, 不超时
		WsWriteTimeout:  0, // 0秒, 不超时
		SendQueueSize:   512,

		UDPConnIdleTimeout: 5 * time.Minute, // 5分钟
		UDPCleanupInterval: 1 * time.Minute, // 1分钟
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
			o.WsOriginChecker = checker
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
			if o.ShutdownTimeout < 50*time.Millisecond { // 最小关闭超时时间，防止压根没效果
				o.ShutdownTimeout = 50 * time.Millisecond
			}
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

// WithWsReadTimeout sets the read timeout for WebSocket connections.
// Set to 0 to disable read timeout.
func WithWsReadTimeout(timeout time.Duration) Option {
	return func(o *Options) {
		if timeout >= 0 {
			o.WsReadTimeout = timeout
			if o.WsReadTimeout < 50*time.Millisecond { // 最小读超时时间，防止压根没效果
				o.WsReadTimeout = 50 * time.Millisecond
			}
		}
	}
}

// WithWsWriteTimeout sets the write timeout for WebSocket connections.
// Set to 0 to disable write timeout.
func WithWsWriteTimeout(timeout time.Duration) Option {
	return func(o *Options) {
		if timeout >= 0 {
			o.WsWriteTimeout = timeout
			if o.WsWriteTimeout < 50*time.Millisecond { // 最小写超时时间，防止压根没效果
				o.WsWriteTimeout = 50 * time.Millisecond
			}
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

// WithSendQueueSize sets the send queue size for WebSocket connections.
func WithSendQueueSize(size int) Option {
	return func(o *Options) {
		if size > 0 {
			o.SendQueueSize = size
		}
	}
}

// WithSendQueueNeedFlushOver sets tcp client need flush over.
func WithSendQueueNeedFlushOver(needFlushOver bool) Option {
	return func(o *Options) {
		o.SendQueueNeedFlushOver = needFlushOver
	}
}

// WithSendQueueTimeoutFlushOver sets timeout send queue flush over.
func WithSendQueueTimeoutFlushOver(timeout time.Duration) Option {
	return func(o *Options) {
		if timeout > 0 {
			o.SendQueueTimeoutFlushOver = timeout
			if o.SendQueueTimeoutFlushOver < 50*time.Millisecond { // 最小超时时间，防止压根没效果
				o.SendQueueTimeoutFlushOver = 50 * time.Millisecond
			}
		}
	}
}

// WithSendQueueFlushTimeoutCallback sets flush timeout callback.
func WithSendQueueFlushTimeoutCallback(cb func(conn IConn, timeout time.Duration)) Option {
	return func(o *Options) {
		o.SendQueueFlushTimeoutCallback = cb
	}
}

// WithIsNeedReconnect sets is need reconnect.
func WithIsNeedReconnect(isNeedReconnect bool) Option {
	return func(o *Options) {
		o.IsNeedReconnect = isNeedReconnect
	}
}

// WithReconnectInterval sets reconnect interval.
func WithReconnectInterval(interval time.Duration, maxRetries int) Option {
	return func(o *Options) {
		if interval > 0 {
			o.ReconnectInterval = interval
			if interval < 500*time.Millisecond { // 最小间隔，防止频繁重连
				o.ReconnectInterval = 500 * time.Millisecond
			}
		}
		o.ReconnectMaxRetries = maxRetries
	}
}

// WithReconnectCallback sets reconnect callback.
func WithReconnectCallback(cb func(attempt int, err error)) Option {
	return func(o *Options) {
		o.ReconnectCallback = cb
	}
}
