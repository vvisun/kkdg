package kknet

import (
	"crypto/tls"
	"net/http"
	"time"

	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/kklog"
)

type OriginCheckFunc func(r *http.Request) bool

// Options are common network settings.
type Options struct {
	Logger              kklog.ILogger                // 日志记录器
	ReadBufferSize      int                          // 读缓冲区大小 1-64KB
	WriteBufferSize     int                          // 写缓冲区大小 1-64KB
	ShutdownTimeout     time.Duration                // 服务关闭超时时间
	IsNeedReconnect     bool                         // 是否需要重连
	ReconnectInterval   time.Duration                // 重连间隔
	ReconnectMaxRetries int                          // 重连最大次数(<=0为无限)
	ReconnectCallback   func(attempt int, err error) // 重连回调(成功时 err 为 nil)
	ReadTimeout         time.Duration                // 读超时时间（为0时，不启用读超时）
	WriteTimeout        time.Duration                // 写超时时间（为0时，不启用写超时）
	PingInterval        time.Duration                // Ping发送间隔（为0时，不发送 Ping）；配合 ReadTimeout 做保活，收到 Pong 会刷新读超时
	TLSConfig           *tls.Config                  // TLS配置。use for wss or tcp with tls

	WpOptions  WriteOptions // 写处理器选项
	RpOptions  ReadOptions  // 读处理器选项
	WpProvider WpProvider   // 写处理器提供者
	RpProvider RpProvider   // 读处理器提供者

	UDPConnIdleTimeout time.Duration // UDP连接空闲超时时间（为0时，不启用空闲清理）
	UDPCleanupInterval time.Duration // UDP清理间隔时间（为0时，不启用清理）

	WsOriginChecker OriginCheckFunc // websocket原始检查器
}

func defaultWSOriginChecker(r *http.Request) bool {
	return true
}

// Option applies changes to Options.
type Option func(*Options)

// DefaultOptions returns default settings.
func DefaultOptions() Options {
	return Options{
		Logger:              kklog.GetConsoleLogger(),
		ReadBufferSize:      4 * 1024,
		WriteBufferSize:     4 * 1024,
		TLSConfig:           nil,
		ShutdownTimeout:     10 * time.Second, // 10秒
		IsNeedReconnect:     true,
		ReconnectInterval:   1 * time.Second,
		ReconnectMaxRetries: 5,
		ReconnectCallback:   nil,

		WsOriginChecker: defaultWSOriginChecker,
		ReadTimeout:     20 * time.Second,
		WriteTimeout:    5 * time.Second,
		PingInterval:    5 * time.Second,

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

	// 读写缓冲区大小。太大连接数一多内存消耗非常高。太小影响性能。
	if opts.ReadBufferSize < 1024 {
		opts.ReadBufferSize = 1024
	}
	if opts.ReadBufferSize > 32*1024 {
		opts.ReadBufferSize = 32 * 1024
	}
	if opts.WriteBufferSize < 1024 {
		opts.WriteBufferSize = 1024
	}
	if opts.WriteBufferSize > 32*1024 {
		opts.WriteBufferSize = 32 * 1024
	}

	if opts.ReadTimeout > 0 && opts.ReadTimeout < 1*time.Second {
		opts.ReadTimeout = 1 * time.Second
	}
	if opts.WriteTimeout > 0 && opts.WriteTimeout < 1*time.Second {
		opts.WriteTimeout = 1 * time.Second
	}
	if opts.PingInterval > 0 && opts.PingInterval < 3*time.Second {
		opts.PingInterval = 3 * time.Second
	}

	if opts.ReconnectInterval > 0 && opts.ReconnectInterval < 500*time.Millisecond {
		opts.ReconnectInterval = 500 * time.Millisecond
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

	CheckWriteOptions(&opts.WpOptions)
	CheckReadOptions(&opts.RpOptions)
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

// WithReadTimeout sets the read timeout for WebSocket connections.
// Set to 0 to disable read timeout.
func WithReadTimeout(timeout time.Duration) Option {
	return func(o *Options) {
		if timeout >= 0 {
			o.ReadTimeout = timeout
			if o.ReadTimeout < 50*time.Millisecond { // 最小读超时时间，防止压根没效果
				o.ReadTimeout = 50 * time.Millisecond
			}
		}
	}
}

// WithWriteTimeout sets the write timeout for WebSocket connections.
// Set to 0 to disable write timeout.
func WithWriteTimeout(timeout time.Duration) Option {
	return func(o *Options) {
		if timeout >= 0 {
			o.WriteTimeout = timeout
			if o.WriteTimeout < 50*time.Millisecond {
				o.WriteTimeout = 50 * time.Millisecond
			}
		}
	}
}

// WithPingInterval sets the interval for sending WebSocket Ping frames (keepalive).
// Set to 0 to disable. When > 0, the connection sends Ping periodically; receiving Pong
// refreshes the read deadline (if WsReadTimeout > 0), so idle connections stay open.
// Minimum 3s to avoid excessive traffic. Typically use with WithWsReadTimeout (e.g. 30s).
func WithPingInterval(interval time.Duration) Option {
	return func(o *Options) {
		if interval >= 0 {
			o.PingInterval = interval
			if o.PingInterval > 0 && o.PingInterval < 2*time.Second {
				o.PingInterval = 2 * time.Second
			}
		}
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

// WithWsOriginChecker sets origin checker.
func WithWsOriginChecker(checker OriginCheckFunc) Option {
	return func(o *Options) {
		if checker != nil {
			o.WsOriginChecker = checker
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

//------------------------- read write processor -------------------------

func WithWpProvider(provider WpProvider) Option {
	return func(o *Options) {
		o.WpProvider = provider
	}
}

func WithRpProvider(provider RpProvider) Option {
	return func(o *Options) {
		o.RpProvider = provider
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

// WithMsgPacket sets message packet.
func WithMsgPacket(packet *kkpacket.MessagePacket) Option {
	return func(o *Options) {
		o.WpOptions.MsgPacket = packet
	}
}
