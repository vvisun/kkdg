package kknet

import (
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/kklog"
)

type ReadOptions struct {
	StreamTool kkpacket.IPacket
	//消费函数, data: [length,message], 外部自行用解码器解码（内置的解码器见kkpacket）
	RawHandler IRawHandler
	//消费函数, data: [length,message], 如果同步调用已经快过拷贝，可以直接同步消费数据。
	NoneCopyHandler INoneCopyHandler

	//接收队列是否严格容量控制
	RecvQueueStrict bool
	//接收队列大小。默认 256。
	// 需配合 RecvQueueStrict为 true 使用，否则队列会自动扩容不会满。这里设置的值会忽略。
	RecvQueueSize int
	// RecvQueue full 回调。当 Push 因队列满失败时调用。
	// 需配合 RecvQueueStrict为 true 使用，否则队列会自动扩容不会满。这里设置的值会忽略。
	// 例如，可以在回调里限流/向客户端发送提示“服务器繁忙”等。
	RecvQueueFullCallback func(conn IConn)
	//当拆包缓冲区 cap 超过该值且当前为空时，会缩容到默认值。防止内存浪费。
	RecvBufShrinkCap int
	// WorkerQueue 最大并发数。用于TaskReadProcessor。
	// 默认 1，表示不并发，保证顺序性。大于1时并发，不保证顺序性。
	// 取值范围会自动归一化到 [1,64]。
	WorkerQueueMaxConcurrency int32
}

func DefaultReadOptions() ReadOptions {
	return ReadOptions{
		StreamTool:                kkpacket.DefaultStreamPacket(),
		RecvQueueSize:             256,
		RecvQueueStrict:           false,
		RecvBufShrinkCap:          2 * 1024, // 2KB
		WorkerQueueMaxConcurrency: 1,
	}
}

func CheckReadOptions(opts *ReadOptions) {
	if opts == nil {
		return
	}
	if opts.StreamTool == nil {
		kklog.Debugf("rp StreamTool is nil, use default stream tool")
		opts.StreamTool = kkpacket.DefaultStreamPacket()
	}
	if opts.RecvQueueSize <= 0 {
		kklog.Debugf("rp RecvQueueSize fixed from %d to %d", opts.RecvQueueSize, 256)
		opts.RecvQueueSize = 256
	}
	if opts.RecvBufShrinkCap <= 0 || opts.RecvBufShrinkCap > 2*1024 {
		kklog.Debugf("rp RecvBufShrinkCap fixed from %d to %d", opts.RecvBufShrinkCap, 2048)
		opts.RecvBufShrinkCap = 2 * 1024 // 2KB
	}
	if opts.WorkerQueueMaxConcurrency <= 0 {
		kklog.Debugf("rp WorkerQueueMaxConcurrency fixed from %d to %d", opts.WorkerQueueMaxConcurrency, 1)
		opts.WorkerQueueMaxConcurrency = 1
	}
	if opts.WorkerQueueMaxConcurrency > 64 {
		kklog.Debugf("rp WorkerQueueMaxConcurrency fixed from %d to %d", opts.WorkerQueueMaxConcurrency, 64)
		opts.WorkerQueueMaxConcurrency = 64
	}
}

// WithRawHandler sets raw handler.
func WithRawHandler(handler IRawHandler) Option {
	return func(o *Options) {
		o.RpOptions.RawHandler = handler
	}
}

// WithNoneCopyHandler sets none copy handler.
func WithNoneCopyHandler(handler INoneCopyHandler) Option {
	return func(o *Options) {
		o.RpOptions.NoneCopyHandler = handler
	}
}

// WithRecvQueueSize sets recv queue size.
// 默认 256，RecvQueueStrict 为 true 时，会有队列满回调。否则会自动扩容，这里设置的值会忽略。
func WithRecvQueueSize(size int) Option {
	return func(o *Options) {
		if size > 0 {
			o.RpOptions.RecvQueueSize = size
		}
	}
}

// WithRecvQueueStrict sets recv queue strict.
func WithRecvQueueStrict(strict bool) Option {
	return func(o *Options) {
		o.RpOptions.RecvQueueStrict = strict
	}
}

// WithRecvBufShrinkCap sets recv buf shrink cap.
func WithRecvBufShrinkCap(cap int) Option {
	return func(o *Options) {
		o.RpOptions.RecvBufShrinkCap = cap
	}
}

// WithWorkerQueueMaxConcurrency sets worker queue max concurrency.
func WithWorkerQueueMaxConcurrency(concurrency int32) Option {
	return func(o *Options) {
		o.RpOptions.WorkerQueueMaxConcurrency = concurrency
	}
}

// WithRecvQueueFullCallback 设置 RecvQueue 满时的回调。
// 回调在 Push 因队列满失败时触发，用于统计、限流或踢连接等。
// 需配合 WithRecvQueueStrict(true) 使用，否则队列会自动扩容不会满。
// 提示：“服务器繁忙” 或 “客户端发送过于频繁”
func WithRecvQueueFullCallback(callback func(conn IConn)) Option {
	return func(o *Options) {
		o.RpOptions.RecvQueueFullCallback = callback
	}
}
