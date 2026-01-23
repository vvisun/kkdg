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
	Logger          kklog.ILogger // 日志记录器
	PoolSize        int           // ants池大小（注意：为0时，不使用ants池。建议使用，以提高性能。默认为CPU核心数）
	ReadBufferSize  int           // 读缓冲区大小
	WriteBufferSize int           // 写缓冲区大小
	TLSConfig       *tls.Config   // TLS配置
	ShutdownTimeout time.Duration // 服务关闭超时时间
	Middlewares     []Middleware  // 中间件列表

	WsOriginChecker OriginCheckFunc // websocket原始检查器
	WsReadTimeout   time.Duration   // WebSocket读超时时间（为0时，不启用读超时）
	WsWriteTimeout  time.Duration   // WebSocket写超时时间（为0时，不启用写超时）

	UDPConnIdleTimeout time.Duration // UDP连接空闲超时时间（为0时，不启用空闲清理）
	UDPCleanupInterval time.Duration // UDP清理间隔时间（为0时，不启用清理）

	TcpClientNeedFlushOver        bool                                    //tcp客户端关闭时是否需要等待 flush 完成
	TcpTimeoutFlushOver           time.Duration                           //tcp客户端关闭时等待 flush 完成的超时时间
	TcpClientFlushTimeoutCallback func(conn IConn, timeout time.Duration) //flush 超时回调
	TcpClientSendQueueSize        int                                     //tcp客户端发送队列初始容量
	TcpClientNeedReconnect        bool                                    //tcp客户端是否需要重连
	TcpClientReconnectInterval    time.Duration                           //tcp客户端重连间隔
	TcpClientReconnectMaxRetries  int                                     //tcp客户端重连最大次数(<=0为无限)
	TcpClientReconnectCallback    func(attempt int, err error)            //tcp客户端重连回调(成功时 err 为 nil)
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
		Logger:                 kklog.Nop(),
		PoolSize:               xos.NumCPU(),
		ReadBufferSize:         defaultBufferSize,
		WriteBufferSize:        defaultBufferSize,
		TLSConfig:              nil,
		WsOriginChecker:        defaultOriginChecker,
		ShutdownTimeout:        defaultShutdownTimeout,
		UDPConnIdleTimeout:     defaultUDPConnIdleTimeout,
		UDPCleanupInterval:     defaultUDPCleanupInterval,
		WsReadTimeout:          defaultReadTimeout,
		WsWriteTimeout:         defaultWriteTimeout,
		TcpClientSendQueueSize: 64,
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
			o.WsReadTimeout = timeout
		}
	}
}

// WithWriteTimeout sets the write timeout for WebSocket connections.
// Set to 0 to disable write timeout.
func WithWriteTimeout(timeout time.Duration) Option {
	return func(o *Options) {
		if timeout >= 0 {
			o.WsWriteTimeout = timeout
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

// WithTcpClientNeedFlushOver sets tcp client need flush over.
func WithTcpClientNeedFlushOver(needFlushOver bool) Option {
	return func(o *Options) {
		o.TcpClientNeedFlushOver = needFlushOver
	}
}

// WithTimeoutTcpFlushOver sets timeout tcp flush over.
func WithTimeoutTcpFlushOver(timeout time.Duration) Option {
	return func(o *Options) {
		if timeout > 0 {
			o.TcpTimeoutFlushOver = timeout
		}
	}
}

// WithTcpClientFlushTimeoutCallback sets flush timeout callback.
func WithTcpClientFlushTimeoutCallback(cb func(conn IConn, timeout time.Duration)) Option {
	return func(o *Options) {
		o.TcpClientFlushTimeoutCallback = cb
	}
}

// WithTcpClientSendQueueSize sets tcp client send queue initial size.
func WithTcpClientSendQueueSize(size int) Option {
	return func(o *Options) {
		if size > 0 {
			o.TcpClientSendQueueSize = size
		}
	}
}

// WithTcpClientReconnect sets tcp client reconnect settings.
func WithTcpClientReconnect(enable bool, interval time.Duration, maxRetries int) Option {
	return func(o *Options) {
		o.TcpClientNeedReconnect = enable
		if interval > 0 {
			o.TcpClientReconnectInterval = interval
		}
		o.TcpClientReconnectMaxRetries = maxRetries
	}
}

// WithTcpClientReconnectCallback sets reconnect callback.
func WithTcpClientReconnectCallback(cb func(attempt int, err error)) Option {
	return func(o *Options) {
		o.TcpClientReconnectCallback = cb
	}
}
