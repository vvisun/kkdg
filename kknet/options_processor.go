package kknet

import (
	"time"
)

type WriteOptions struct {
	//发送队列大小
	SendQueueSize int
	//发送队列是否严格容量控制
	SendQueueStrict bool
	//关闭时是否需要等待 flush 完成。
	// 客户端没必要等待 flush 完成，因为实际中客户端会是web/app/小程序等，
	// 服务端 以及 rpc中的client端一般需要等待 flush 完成。
	SendQueueNeedFlushOver bool
	//关闭时等待 flush 完成的超时时间
	SendQueueTimeoutFlushOver time.Duration
	//flush 超时回调
	SendQueueFlushTimeoutCallback func(conn IConn, timeout time.Duration)
	//每轮持锁时最多 Pop 的帧数，减少 Lock 次数与 Send 竞争，同时会创建这个大小的缓存复用以减少内存分配
	WriteBatchSize int
	//单次批量写入的最大字节数(<=0 不限制)
	WriteBatchLimitBytes int
}

func DefaultWriteOptions() WriteOptions {
	return WriteOptions{
		SendQueueSize:             256,
		SendQueueStrict:           false,
		SendQueueNeedFlushOver:    true,
		SendQueueTimeoutFlushOver: 5 * time.Second,
		WriteBatchSize:            32,
		WriteBatchLimitBytes:      1024,
	}
}

func CheckWriteOptions(opts *WriteOptions) {
	if opts == nil {
		return
	}
	if opts.SendQueueSize <= 0 {
		opts.SendQueueSize = 256
	}
	if opts.WriteBatchSize < 8 {
		opts.WriteBatchSize = 8
	}
	if opts.WriteBatchSize > 32 {
		opts.WriteBatchSize = 32
	}
	if opts.WriteBatchLimitBytes < 512 {
		opts.WriteBatchLimitBytes = 512
	}
	if opts.WriteBatchLimitBytes > 4096 {
		opts.WriteBatchLimitBytes = 4096
	}
}

type ReadOptions struct {
	//消费函数, msg: 解码好的object, msgID: 消息ID
	MsgHandler IMsgHandler
	//消费函数, data: [length,message], 外部自行用解码器解码（内置的解码器见kkpacket/parser.go）
	RawHandler IRawHandler
	//消费函数, data: [length,message], 如果同步调用已经快过拷贝，可以直接同步消费数据。
	NoneCopyHandler INoneCopyHandler
	//接收队列大小
	RecvQueueSize int
	//接收队列是否严格容量控制
	RecvQueueStrict bool
	//每轮消费最多 Pop 的帧数，减少 Lock 次数与消费竞争，同时会创建这个大小的缓存复用以减少内存分配
	RecvBatchSize int
	//当 recvBuf cap 超过该值且当前为空时，缩容到默认值(<=0 使用默认值defaultRecvBufSize)
	RecvBufShrinkCap int
}

func DefaultReadOptions() ReadOptions {
	return ReadOptions{
		RecvQueueSize:    256,
		RecvQueueStrict:  false,
		RecvBatchSize:    8,
		RecvBufShrinkCap: 2 * 1024, // 2KB
	}
}

func CheckReadOptions(opts *ReadOptions) {
	if opts == nil {
		return
	}
	if opts.RecvQueueSize <= 0 {
		opts.RecvQueueSize = 256
	}
	if opts.RecvBatchSize < 8 {
		opts.RecvBatchSize = 8
	}
	if opts.RecvBufShrinkCap <= 0 {
		// 空闲时如果 cap 过大则缩容，避免长期占用大内存。
		opts.RecvBufShrinkCap = 2 * 1024 // 2KB
	}
}
