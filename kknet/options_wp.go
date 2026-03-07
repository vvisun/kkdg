package kknet

import (
	"time"

	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/kklog"
)

type EWpQueueFullAction int

const (
	EWpQueueFullActionDrop  EWpQueueFullAction = iota // 丢弃
	EWpQueueFullActionBlock                           // 阻塞
	EWpQueueFullActionRetry                           // 重试
)

type WriteOptions struct {
	// 消息包解码器
	MsgPacket *kkpacket.MessagePacket
	// 发送队列大小
	SendQueueSize int
	// 发送队列是否严格容量控制
	SendQueueStrict bool
	// 关闭时是否需要等待 flush 完成。
	// 客户端没必要等待 flush 完成，因为实际中客户端会是web/app/小程序等，
	// 服务端 以及 rpc中的client端一般需要等待 flush 完成。
	SendQueueNeedFlushOver bool
	// 关闭时等待 flush 完成的超时时间
	SendQueueTimeoutFlushOver time.Duration
	// flush 超时回调
	SendQueueFlushTimeoutCallback func(conn IConn, timeout time.Duration)
	// 单次批量写入的帧数。会创建这个大小的缓存数组以复用实现零分配
	BatchWriteSize int
	// 单次批量写入的最大字节数(<=0 不限制)
	BatchWriteLimitBytes int
	// SendQueue full 动作
	SendQueueFullAction EWpQueueFullAction
	// Retry 模式：重试间隔（默认 2ms）
	SendQueueRetryInterval time.Duration
	// Retry 模式：最大重试次数（0 表示无限，默认 100）
	SendQueueRetryMaxCount int
	// writeFn 失败时的最大重试次数（0 表示不重试，直接放弃并关闭写协程）
	WriteFnRetryMaxCount int
	// writeFn 失败时的重试间隔（默认 5ms）
	WriteFnRetryInterval time.Duration
	// writeFn 失败时是否可重试。nil 时使用默认逻辑（连接已关闭等致命错误不重试）
	WriteFnIsRetryable func(err error) bool
}

func DefaultWriteOptions() WriteOptions {
	return WriteOptions{
		SendQueueSize:             128,
		SendQueueStrict:           false,
		SendQueueNeedFlushOver:    true,
		SendQueueTimeoutFlushOver: 5 * time.Second,
		BatchWriteSize:            32,
		BatchWriteLimitBytes:      1024,
		SendQueueFullAction:       EWpQueueFullActionDrop,
		SendQueueRetryInterval:    2 * time.Millisecond,
		SendQueueRetryMaxCount:    100,
		WriteFnRetryMaxCount:      0,
		WriteFnRetryInterval:      5 * time.Millisecond,
	}
}

func CheckWriteOptions(opts *WriteOptions) {
	if opts == nil {
		return
	}
	if opts.SendQueueSize <= 0 {
		kklog.Debugf("wp SendQueueSize fixed from %d to %d", opts.SendQueueSize, 128)
		opts.SendQueueSize = 128
	}
	if opts.BatchWriteSize < 8 {
		kklog.Debugf("wp BatchWriteSize fixed from %d to %d", opts.BatchWriteSize, 8)
		opts.BatchWriteSize = 8
	}
	if opts.BatchWriteSize > 64 {
		kklog.Debugf("wp BatchWriteSize fixed from %d to %d", opts.BatchWriteSize, 64)
		opts.BatchWriteSize = 64
	}
	if opts.BatchWriteLimitBytes < 512 {
		kklog.Debugf("wp BatchWriteLimitBytes fixed from %d to %d", opts.BatchWriteLimitBytes, 512)
		opts.BatchWriteLimitBytes = 512
	}
	if opts.BatchWriteLimitBytes > 4096 {
		kklog.Debugf("wp BatchWriteLimitBytes fixed from %d to %d", opts.BatchWriteLimitBytes, 4096)
		opts.BatchWriteLimitBytes = 4096
	}
	if opts.SendQueueRetryInterval <= 0 {
		kklog.Debugf("wp SendQueueRetryInterval fixed from %d to %d", opts.SendQueueRetryInterval, 2*time.Millisecond)
		opts.SendQueueRetryInterval = 2 * time.Millisecond
	}
	if opts.SendQueueRetryMaxCount < 0 {
		kklog.Debugf("wp SendQueueRetryMaxCount fixed from %d to %d", opts.SendQueueRetryMaxCount, 0)
		opts.SendQueueRetryMaxCount = 0
	}
	if opts.WriteFnRetryMaxCount < 0 {
		kklog.Debugf("wp WriteFnRetryMaxCount fixed from %d to %d", opts.WriteFnRetryMaxCount, 0)
		opts.WriteFnRetryMaxCount = 0
	}
	if opts.WriteFnRetryInterval <= 0 {
		kklog.Debugf("wp WriteFnRetryInterval fixed from %d to %d", opts.WriteFnRetryInterval, 5*time.Millisecond)
		opts.WriteFnRetryInterval = 5 * time.Millisecond
	}
	if opts.SendQueueTimeoutFlushOver > 0 && opts.SendQueueTimeoutFlushOver < 500*time.Millisecond {
		kklog.Debugf("wp SendQueueTimeoutFlushOver fixed from %d to %d", opts.SendQueueTimeoutFlushOver, 500*time.Millisecond)
		opts.SendQueueTimeoutFlushOver = 500 * time.Millisecond
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

// WithSendQueueStrict sets send queue strict.
func WithSendQueueStrict(strict bool) Option {
	return func(o *Options) {
		o.WpOptions.SendQueueStrict = strict
	}
}

// WithSendQueueNeedFlushOver sets send queue need flush over.
func WithSendQueueNeedFlushOver(needFlushOver bool) Option {
	return func(o *Options) {
		o.WpOptions.SendQueueNeedFlushOver = needFlushOver
	}
}

// WithSendQueueTimeoutFlushOver sets send queue timeout flush over.
func WithSendQueueTimeoutFlushOver(timeout time.Duration) Option {
	return func(o *Options) {
		o.WpOptions.SendQueueTimeoutFlushOver = timeout
	}
}

// WithSendQueueFlushTimeoutCallback sets send queue flush timeout callback.
func WithSendQueueFlushTimeoutCallback(cb func(conn IConn, timeout time.Duration)) Option {
	return func(o *Options) {
		o.WpOptions.SendQueueFlushTimeoutCallback = cb
	}
}

// WithBatchWriteSize sets batch write size.
func WithBatchWriteSize(size int) Option {
	return func(o *Options) {
		o.WpOptions.BatchWriteSize = size
	}
}

// WithBatchWriteLimitBytes sets batch write limit bytes.
func WithBatchWriteLimitBytes(limit int) Option {
	return func(o *Options) {
		o.WpOptions.BatchWriteLimitBytes = limit
	}
}

// WithMsgPacket sets message packet.
func WithMsgPacket(packet *kkpacket.MessagePacket) Option {
	return func(o *Options) {
		o.WpOptions.MsgPacket = packet
	}
}

// WithSendQueueFullAction sets send queue full action.
func WithSendQueueFullAction(action EWpQueueFullAction) Option {
	return func(o *Options) {
		o.WpOptions.SendQueueFullAction = action
	}
}

// WithSendQueueRetryInterval sets retry interval for Retry mode.
func WithSendQueueRetryInterval(interval time.Duration) Option {
	return func(o *Options) {
		if interval > 0 {
			o.WpOptions.SendQueueRetryInterval = interval
		}
	}
}

// WithSendQueueRetryMaxCount sets max retry count for Retry mode (0 = infinite).
func WithSendQueueRetryMaxCount(max int) Option {
	return func(o *Options) {
		if max >= 0 {
			o.WpOptions.SendQueueRetryMaxCount = max
		}
	}
}

// WithWriteFnRetryMaxCount 设置 writeFn 失败时的最大重试次数。
// 0 表示不重试，失败后直接放弃并关闭写协程（默认行为）。
func WithWriteFnRetryMaxCount(max int) Option {
	return func(o *Options) {
		if max >= 0 {
			o.WpOptions.WriteFnRetryMaxCount = max
		}
	}
}

// WithWriteFnRetryInterval 设置 writeFn 失败时的重试间隔（默认 5ms）。
func WithWriteFnRetryInterval(interval time.Duration) Option {
	return func(o *Options) {
		if interval > 0 {
			o.WpOptions.WriteFnRetryInterval = interval
		}
	}
}

// WithWriteFnIsRetryable 设置 writeFn 失败时是否可重试的判断函数。
// nil 时使用默认逻辑（ErrConnectionClosed、net.ErrClosed、io.ErrClosedPipe 等不重试）。
func WithWriteFnIsRetryable(fn func(err error) bool) Option {
	return func(o *Options) {
		o.WpOptions.WriteFnIsRetryable = fn
	}
}
