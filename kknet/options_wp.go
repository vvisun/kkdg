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
	// 消息包解码器
	MsgPacket *kkpacket.MessagePacket
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
