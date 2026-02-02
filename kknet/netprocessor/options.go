package netprocessor

import (
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
)

type WriteOptions struct {
	SendQueueSize                 int                                           //发送队列大小
	SendQueueStrict               bool                                          //发送队列是否严格容量控制
	SendQueueNeedFlushOver        bool                                          //关闭时是否需要等待 flush 完成
	SendQueueTimeoutFlushOver     time.Duration                                 //关闭时等待 flush 完成的超时时间
	SendQueueFlushTimeoutCallback func(conn kknet.IConn, timeout time.Duration) //flush 超时回调
	WriteBatchSize                int                                           //每轮持锁时最多 Pop 的帧数，减少 Lock 次数与 Send 竞争
	WriteBatchLimitBytes          int                                           //单次批量写入的最大字节数(<=0 不限制)
}

func CheckWriteOptions(opts *WriteOptions) {
	if opts == nil {
		return
	}
	if opts.SendQueueSize <= 0 {
		opts.SendQueueSize = 1024
	}
	if opts.WriteBatchSize <= 0 {
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
	RecvQueueSize    int               //接收队列大小
	RecvQueueStrict  bool              //接收队列是否严格容量控制
	MsgHandler       kknet.IMsgHandler //消费函数, msg: object, msgID: 消息ID
	RawHandler       kknet.IRawHandler //消费函数, data: [length,message], 外部自行用解码器解码（内置的解码器见kkpacket/message_parser.go）
	RecvBatchSize    int               //每轮消费最多 Pop 的帧数
	RecvBufShrinkCap int               //当 recvBuf cap 超过该值且当前为空时，缩容到默认值(<=0 使用默认值)
}

func CheckReadOptions(opts *ReadOptions) {
	if opts == nil {
		return
	}
	if opts.RecvQueueSize <= 0 {
		opts.RecvQueueSize = 1024
	}
	if opts.RecvBatchSize <= 0 {
		opts.RecvBatchSize = 32
	}
	if opts.RecvBufShrinkCap <= 0 {
		// 空闲时如果 cap 过大则缩容，避免长期占用大内存。
		opts.RecvBufShrinkCap = 4 * kkpacket.DefaultMaxMessageSize()
	}
}
