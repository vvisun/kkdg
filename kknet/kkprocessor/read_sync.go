package kkprocessor

import (
	"sync"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/byteslice"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/xcall"
)

// 消息处理器-接收器。
//
//	同步消费数据，实现0拷贝优化。NoneCopyHandler必须设置，RawHandler会忽略。
//	保证顺序性，适合NoneCopyHandler逻辑非常轻的场景。
//
// 主动关闭Server或Client后，只消费，不再接受数据入队。
//
//	此外，也可以让外部自行选择，是否启动携程来消费数据。
//	注意，外部如果启动携程，需要自己拷贝数据，因为底层会覆盖数据，携程中的数据不再是原始数据。
type SyncReadProcessor struct {
	conn   kknet.IConn
	connID kknet.CONN_ID     //连接ID，记录下来，方便conn关闭导致conn为空时，消费携程可以继续消费。
	opts   kknet.ReadOptions //选项

	recvBuf  []byte                        //残包缓冲区。初始化为nil，避免永远没残包还一直占内存。有残包再分配即可。
	splitBuf [kknet.BatchPacketSize][]byte //拆分缓冲区，用于拆分数据包时复用，避免分配新的内存

	mu sync.Mutex
}

var _ kknet.IReadProcessor = (*SyncReadProcessor)(nil)

func NewSyncReadProcessor(opts kknet.ReadOptions) kknet.IReadProcessor {
	kknet.CheckReadOptions(&opts)
	if opts.NoneCopyHandler == nil {
		kklog.PanicLog("NoneCopyHandler is required")
	}

	return &SyncReadProcessor{
		recvBuf: nil,
		opts:    opts,
	}
}

func (rp *SyncReadProcessor) Pending() int {
	return 0
}

func (rp *SyncReadProcessor) Start(conn kknet.IConn) {
	rp.conn = conn
	rp.connID = conn.ID()
}

func (rp *SyncReadProcessor) Stop() {
	rp.conn = nil
}

func (rp *SyncReadProcessor) EnqueuePacket(packet []byte) {
	if len(packet) == 0 {
		return
	}
	rp.mu.Lock()
	xcall.SafeCall(func() {
		rp.opts.NoneCopyHandler.OnNoneCopy(rp.connID, packet)
	})
	rp.mu.Unlock()
}

func (rp *SyncReadProcessor) reRecvBuf(capacity int) {
	if rp.recvBuf == nil {
		rp.recvBuf = byteslice.GetZero(capacity)
		return
	}
	rp.recvBuf = rp.recvBuf[:0]
	byteslice.Put(rp.recvBuf)
	rp.recvBuf = byteslice.GetZero(capacity)
}

func (rp *SyncReadProcessor) OnRecvBytes(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	rp.mu.Lock()
	defer rp.mu.Unlock()

	buf := data

	if len(rp.recvBuf) > 0 {
		// 有残包，则将数据拼接到残包后面
		rp.recvBuf = append(rp.recvBuf, data...)
		buf = rp.recvBuf
	}

	// Parse [length,message][length,message]...
	stream := rp.opts.StreamTool
	packets, leftData, err := stream.Split(buf, rp.splitBuf[:0])
	if err != nil {
		if rp.recvBuf != nil {
			rp.recvBuf = rp.recvBuf[:0]
		}
		return err
	}

	// 存下残包，下次收到数据时拼接到后面。
	if len(leftData) > 0 {
		leftLen := len(leftData)
		rp.reRecvBuf(defaultRecvBufSize + leftLen)
		rp.recvBuf = rp.recvBuf[:leftLen]
		copy(rp.recvBuf, leftData)
	}

	// shrink: if empty and cap too big, shrink to default.
	if len(rp.recvBuf) == 0 && cap(rp.recvBuf) > rp.opts.RecvBufShrinkCap {
		byteslice.Put(rp.recvBuf)
		rp.recvBuf = nil
	}

	// 同步消费数据，实现0拷贝优化。
	xcall.SafeCall(func() {
		for _, packet := range packets {
			rp.opts.NoneCopyHandler.OnNoneCopy(rp.connID, packet)
		}
	})

	return nil
}
