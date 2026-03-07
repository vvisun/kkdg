package kknet

import "github.com/vvisun/kkdg/utils/kklog"

type ReadOptions struct {
	//消费函数, data: [length,message], 外部自行用解码器解码（内置的解码器见kkpacket）
	RawHandler IRawHandler
	//消费函数, data: [length,message], 如果同步调用已经快过拷贝，可以直接同步消费数据。
	NoneCopyHandler INoneCopyHandler
	//接收队列大小
	RecvQueueSize int
	//接收队列是否严格容量控制
	RecvQueueStrict bool
	//当拆包缓冲区 cap 超过该值且当前为空时，缩容到默认值(<=0 使用默认值defaultRecvBufSize)
	RecvBufShrinkCap int
	// RecvQueue full 回调。当 Push 因队列满失败时调用（需 RecvQueueStrict=true 才会出现队列满）
	RecvQueueFullCallback func(conn IConn)
	// WorkerQueue 最大并发数。用于TaskReadProcessor。
	// 默认 1，表示不并发，保证顺序性。大于1时并发，不保证顺序性。
	// 取值范围会自动归一化到 [1,64]。
	WorkerQueueMaxConcurrency int32
}

func DefaultReadOptions() ReadOptions {
	return ReadOptions{
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
	if opts.RecvQueueSize <= 0 {
		kklog.Debugf("rp RecvQueueSize fixed from %d to %d", opts.RecvQueueSize, 512)
		opts.RecvQueueSize = 512
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
