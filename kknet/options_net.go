package kknet

import (
	"crypto/tls"
	"net/http"
	"time"

	"github.com/vvisun/kkdg/utils/kklog"
)

type OriginCheckFunc func(r *http.Request) bool

// Options are common network settings.
type Options struct {
	Logger              kklog.ILogger                // 日志记录器
	ReadBufferSize      int                          // 读缓冲区大小
	WriteBufferSize     int                          // 写缓冲区大小
	ShutdownTimeout     time.Duration                // 服务关闭超时时间
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

	WpOptions WriteOptions
	RpOptions ReadOptions
}

func defaultWSOriginChecker(r *http.Request) bool {
	return true
}

// Option applies changes to Options.
type Option func(*Options)

// DefaultOptions returns default settings.
func DefaultOptions() Options {
	return Options{
		Logger:              kklog.Nop(),
		ReadBufferSize:      64 * 1024, // 64KB
		WriteBufferSize:     64 * 1024, // 64KB
		TLSConfig:           nil,
		ShutdownTimeout:     10 * time.Second, // 10秒
		IsNeedReconnect:     true,
		ReconnectInterval:   1 * time.Second,
		ReconnectMaxRetries: 5,
		ReconnectCallback:   nil,

		WsOriginChecker: defaultWSOriginChecker,
		WsReadTimeout:   16 * time.Second, // 0秒, 不超时
		WsWriteTimeout:  16 * time.Second, // 0秒, 不超时

		UDPConnIdleTimeout: 5 * time.Minute, // 5分钟
		UDPCleanupInterval: 1 * time.Minute, // 1分钟

		WpOptions: DefaultWriteOptions(),
		RpOptions: DefaultReadOptions(),
	}
}

func CheckOptions(opts *Options) {
	if opts == nil {
		return
	}
	if opts.WsReadTimeout > 0 && opts.WsReadTimeout < 100*time.Millisecond {
		opts.WsReadTimeout = 100 * time.Millisecond
	}
	if opts.WsWriteTimeout > 0 && opts.WsWriteTimeout < 100*time.Millisecond {
		opts.WsWriteTimeout = 100 * time.Millisecond
	}
	if opts.ReconnectInterval > 0 && opts.ReconnectInterval < 500*time.Millisecond {
		opts.ReconnectInterval = 500 * time.Millisecond
	}
	if opts.ReadBufferSize > 0 && opts.ReadBufferSize < 2*1024 {
		opts.ReadBufferSize = 2 * 1024
	}
	if opts.WriteBufferSize > 0 && opts.WriteBufferSize < 2*1024 {
		opts.WriteBufferSize = 2 * 1024
	}
	if opts.ShutdownTimeout > 0 && opts.ShutdownTimeout < 500*time.Millisecond {
		opts.ShutdownTimeout = 500 * time.Millisecond
	}
	if opts.UDPConnIdleTimeout > 0 && opts.UDPConnIdleTimeout < 500*time.Millisecond {
		opts.UDPConnIdleTimeout = 500 * time.Millisecond
	}
	if opts.UDPCleanupInterval > 0 && opts.UDPCleanupInterval < 500*time.Millisecond {
		opts.UDPCleanupInterval = 500 * time.Millisecond
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
	CheckOptions(&cfg)
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

// WithSendQueueSize sets the send queue size for WebSocket connections.
func WithSendQueueSize(size int) Option {
	return func(o *Options) {
		if size > 0 {
			o.WpOptions.SendQueueSize = size
		}
	}
}

// WithSendQueueNeedFlushOver sets tcp client need flush over.
func WithSendQueueNeedFlushOver(needFlushOver bool) Option {
	return func(o *Options) {
		o.WpOptions.SendQueueNeedFlushOver = needFlushOver
	}
}

// WithSendQueueTimeoutFlushOver sets timeout send queue flush over.
func WithSendQueueTimeoutFlushOver(timeout time.Duration) Option {
	return func(o *Options) {
		if timeout > 0 {
			o.WpOptions.SendQueueTimeoutFlushOver = timeout
			if o.WpOptions.SendQueueTimeoutFlushOver < 50*time.Millisecond { // 最小超时时间，防止压根没效果
				o.WpOptions.SendQueueTimeoutFlushOver = 50 * time.Millisecond
			}
		}
	}
}

// WithSendQueueFlushTimeoutCallback sets flush timeout callback.
func WithSendQueueFlushTimeoutCallback(cb func(conn IConn, timeout time.Duration)) Option {
	return func(o *Options) {
		o.WpOptions.SendQueueFlushTimeoutCallback = cb
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

func WithMsgHandler(handler IMsgHandler) Option {
	return func(o *Options) {
		o.RpOptions.MsgHandler = handler
	}
}

func WithRawHandler(handler IRawHandler) Option {
	return func(o *Options) {
		o.RpOptions.RawHandler = handler
	}
}

func WithNoneCopyHandler(handler INoneCopyHandler) Option {
	return func(o *Options) {
		o.RpOptions.NoneCopyHandler = handler
	}
}

func WithRecvQueueSize(size int) Option {
	return func(o *Options) {
		if size > 0 {
			o.RpOptions.RecvQueueSize = size
		}
	}
}

func WithRecvQueueStrict(strict bool) Option {
	return func(o *Options) {
		o.RpOptions.RecvQueueStrict = strict
	}
}
