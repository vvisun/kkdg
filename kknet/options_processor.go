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
		kklog.Debugf("wp BatchWriteLimitBytes fixed from %d to %d", opts.BatchWriteLimitBytes, 2048)
		opts.BatchWriteLimitBytes = 4096
	}
	if opts.SendQueueRetryInterval <= 0 {
		opts.SendQueueRetryInterval = 2 * time.Millisecond
	}
	if opts.SendQueueRetryMaxCount < 0 {
		opts.SendQueueRetryMaxCount = 0
	}
}

//--------------------------------------------------

type ReadOptions struct {
	//消费函数, data: [length,message], 外部自行用解码器解码（内置的解码器见kkpacket）
	RawHandler IRawHandler
	//消费函数, data: [length,message], 如果同步调用已经快过拷贝，可以直接同步消费数据。
	NoneCopyHandler INoneCopyHandler
	//接收队列大小
	RecvQueueSize int
	//接收队列是否严格容量控制
	RecvQueueStrict bool
	//每轮消费最多 Pop 的帧数，减少 Lock 次数与消费竞争，同时会创建这个大小的缓存数组复用以实现0分配
	RecvBatchSize int
	//当 recvBuf cap 超过该值且当前为空时，缩容到默认值(<=0 使用默认值defaultRecvBufSize)
	RecvBufShrinkCap int
	//RecvQueue full 回调
	RecvQueueFullCallback func(conn IConn)
}

func DefaultReadOptions() ReadOptions {
	return ReadOptions{
		RecvQueueSize:    256,
		RecvQueueStrict:  false,
		RecvBatchSize:    32,
		RecvBufShrinkCap: 2 * 1024, // 2KB
	}
}

func CheckReadOptions(opts *ReadOptions) {
	if opts == nil {
		return
	}
	if opts.RecvQueueSize <= 0 {
		kklog.Debugf("rp RecvQueueSize fixed from %d to %d", opts.RecvQueueSize, 512)
		opts.RecvQueueSize = 512
	}
	if opts.RecvBatchSize < 8 {
		kklog.Debugf("rp RecvBatchSize fixed from %d to %d", opts.RecvBatchSize, 8)
		opts.RecvBatchSize = 8 //太小影响性能
	}
	if opts.RecvBatchSize > 128 {
		kklog.Debugf("rp RecvBatchSize fixed from %d to %d", opts.RecvBatchSize, 128)
		opts.RecvBatchSize = 128 //太大占内存
	}
	if opts.RecvBufShrinkCap <= 0 || opts.RecvBufShrinkCap > 2*1024 {
		kklog.Debugf("rp RecvBufShrinkCap fixed from %d to %d", opts.RecvBufShrinkCap, 2048)
		opts.RecvBufShrinkCap = 2 * 1024 // 2KB
	}
	if opts.NoneCopyHandler != nil && opts.RawHandler != nil {
		kklog.Warnf("rp NoneCopyHandler and RawHandler both set, RawHandler will be ignored")
	}
}
