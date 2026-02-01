package netprocessor

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/queues/bbqueue"
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
	if opts.WriteBatchLimitBytes <= 1024 {
		opts.WriteBatchLimitBytes = 1024
	}
	if opts.WriteBatchLimitBytes > 4096 {
		opts.WriteBatchLimitBytes = 4096
	}
}

type WriteFunc func(batch []*kkbuffer.ByteBuffer, n int) error

/**
 * 消息处理器-发送器
 * 负责编码、然后将编码后的数据投入发送队列，供连接发送。
 */
type WriteProcessor struct {
	conn            kknet.IConn            //连接(用于 flush 超时回调传参)
	connID          kknet.CONN_ID          //连接ID，记录下来，方便conn关闭导致conn为空时，消费携程可以继续消费。
	userID          int64                  //用户ID，记录下来，方便业务逻辑层使用。记录conn绑定的用户ID。
	sendQueue       *bbqueue.BBQueue       //发送队列
	sendBatchBuffer []*kkbuffer.ByteBuffer //批量发送缓冲区

	opts WriteOptions

	sendMu    sync.Mutex
	closeOnce sync.Once
	closing   atomic.Bool

	wakeCh    chan struct{}
	closeCh   chan struct{}
	drainedCh chan struct{}
	doneCh    chan struct{}

	writeFn      WriteFunc
	onWriteError func(error)
}

func NewWriteProcessor(opts WriteOptions) *WriteProcessor {
	CheckWriteOptions(&opts)
	return &WriteProcessor{
		opts:            opts,
		sendQueue:       bbqueue.NewBBQueue(opts.SendQueueSize, opts.SendQueueStrict),
		sendBatchBuffer: make([]*kkbuffer.ByteBuffer, opts.WriteBatchSize),
		wakeCh:          make(chan struct{}, 1),
		closeCh:         make(chan struct{}),
		drainedCh:       make(chan struct{}),
		doneCh:          make(chan struct{}),
	}
}

func (wp *WriteProcessor) Done() <-chan struct{} { return wp.doneCh }

func (wp *WriteProcessor) Pending() int {
	wp.sendMu.Lock()
	defer wp.sendMu.Unlock()
	if wp.sendQueue == nil {
		return 0
	}
	return wp.sendQueue.Len()
}

// Start starts the writer goroutine. writeFn must consume the buffers in batch
// (and clear wp.sendBatchBuffer[0:n] pointers) before returning.
func (wp *WriteProcessor) Start(conn kknet.IConn, writeFn WriteFunc, onWriteError func(error)) {
	wp.conn = conn
	wp.writeFn = writeFn
	wp.onWriteError = onWriteError
	go wp.writeLoop()
}

func (wp *WriteProcessor) SendBuffer(buffer buffers.IBuffer) error {
	wp.sendMu.Lock()
	if wp.closing.Load() {
		wp.sendMu.Unlock()
		kkbuffer.Put(buffer)
		return kkerrors.ErrConnectionClosed
	}
	wasEmpty := wp.sendQueue.IsEmpty()
	ok := wp.sendQueue.Push(buffer)
	wp.sendMu.Unlock()
	if !ok {
		//发送队列已满，返回错误。
		//暂时直接返回，后续可以考虑丢弃/阻塞/...。
		kkbuffer.Put(buffer)
		return kkerrors.ErrSendQueueFull
	}
	if wasEmpty {
		wp.wakeWriter()
	}
	return nil
}

// 发送消息。
func (wp *WriteProcessor) SendMessage(msg any) error {
	// 编码消息
	buffer, err := kkpacket.EncodeStream(msg, kkpacket.DefaultStreamPacket())
	if err != nil {
		return err
	}
	return wp.SendBuffer(buffer)
}

// 唤醒写携程。让写携程消费发送队列中的数据并发送。
func (wp *WriteProcessor) wakeWriter() {
	select {
	case wp.wakeCh <- struct{}{}:
	default:
	}
}

func (wp *WriteProcessor) Stop(err error) {
	wp.closeOnce.Do(func() {
		wp.closing.Store(true)
		flush := wp.opts.SendQueueNeedFlushOver && err == nil
		if flush {
			wp.wakeWriter()
			timeout := wp.opts.SendQueueTimeoutFlushOver
			if timeout <= 0 {
				timeout = 10 * time.Second
			}
			select {
			case <-wp.drainedCh:
			case <-time.After(timeout):
				if wp.opts.SendQueueFlushTimeoutCallback != nil && wp.conn != nil {
					wp.opts.SendQueueFlushTimeoutCallback(wp.conn, timeout)
				}
				close(wp.closeCh)
			}
		} else {
			close(wp.closeCh)
		}
		<-wp.doneCh
	})
}

func (wp *WriteProcessor) drainRelease(n int) {
	for i := 0; i < n; i++ {
		bb := wp.sendBatchBuffer[i]
		wp.sendBatchBuffer[i] = nil
		if bb != nil {
			kkbuffer.Put(bb)
		}
	}
}

// 消费发送队列中的数据并发送。
func (wp *WriteProcessor) writeLoop() {
	defer close(wp.doneCh)

	for {
		select {
		case <-wp.wakeCh:
		case <-wp.closeCh:
			// stop: release everything left in queue
			for {
				wp.sendMu.Lock()
				n := wp.sendQueue.PopMany(len(wp.sendBatchBuffer), wp.sendBatchBuffer, 0)
				wp.sendMu.Unlock()
				if n <= 0 {
					return
				}
				wp.drainRelease(n)
			}
		}

		for {
			wp.sendMu.Lock()
			n := wp.sendQueue.PopMany(len(wp.sendBatchBuffer), wp.sendBatchBuffer, wp.opts.WriteBatchLimitBytes)
			remain := wp.sendQueue.Len()
			closing := wp.closing.Load()
			wp.sendMu.Unlock()

			if n <= 0 {
				if closing && remain == 0 {
					select {
					case <-wp.drainedCh:
					default:
						close(wp.drainedCh)
					}
					return
				}
				break
			}

			if err := wp.writeFn(wp.sendBatchBuffer, n); err != nil {
				// 如果 writeFn 没有自己释放/清理，这里兜底释放，避免泄漏
				wp.drainRelease(n)
				if wp.onWriteError != nil {
					wp.onWriteError(err)
				}
				return
			}
		}
	}
}
