package kkws

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/queues/bbqueue"
)

const writeBatchSize = 16 // 每轮持锁时最多 Pop 的帧数，减少 Lock 次数与 Send 竞争

type wsConn struct {
	id    kknet.CONN_ID
	conn  *websocket.Conn
	opts  kknet.Options
	stats *kknet.Stats

	writeMu sync.Mutex // websocket 写必须串行

	sendMu    sync.Mutex
	closeOnce sync.Once

	closing atomic.Bool

	sendQueue   *bbqueue.BBQueue
	batchBuffer [writeBatchSize]*kkbuffer.ByteBuffer

	wakeCh      chan struct{} // 唤醒 writer（边沿触发）
	closeCh     chan struct{} // 立即停止 writer（不再写出，只回收队列）
	closeChOnce sync.Once

	drainedCh   chan struct{} // flush 完成信号（队列清空且已写出）
	drainedOnce sync.Once

	writeDone chan struct{}

	ctxMu sync.RWMutex
	ctx   context.Context
}

var _ kknet.IConn = (*wsConn)(nil)

func newWSConn(conn *websocket.Conn, opts kknet.Options, stats *kknet.Stats) *wsConn {
	c := &wsConn{
		id:    kknet.NextConnID(),
		conn:  conn,
		opts:  opts,
		stats: stats,
		ctx:   context.Background(),
	}
	c.initSendQueue()
	return c
}

func (c *wsConn) initSendQueue() {
	// 允许通过 options 控制队列行为（size<=0 时使用默认值）
	size := c.opts.SendQueueSize
	if size <= 0 {
		size = kknet.DefaultOptions().SendQueueSize
	}
	// NOTE: bbqueue.NewBBQueue 的 size 含义为 chunkSize，初始容量也等于 chunkSize
	c.sendQueue = bbqueue.NewBBQueue(size, c.opts.SendQueueStrict)

	c.wakeCh = make(chan struct{}, 1)
	c.closeCh = make(chan struct{})
	c.drainedCh = make(chan struct{})
	c.writeDone = make(chan struct{})

	go c.writeLoop()
}

func (c *wsConn) wakeWriter() {
	if c.wakeCh == nil {
		return
	}
	select {
	case c.wakeCh <- struct{}{}:
	default:
	}
}

func (c *wsConn) signalDrained() {
	c.drainedOnce.Do(func() {
		if c.drainedCh != nil {
			close(c.drainedCh)
		}
	})
}

func (c *wsConn) stopWriter() {
	c.closeChOnce.Do(func() {
		if c.closeCh != nil {
			close(c.closeCh)
		}
	})
}

func (c *wsConn) ID() kknet.CONN_ID {
	return c.id
}

func (c *wsConn) RemoteAddr() string {
	if c.conn == nil || c.conn.UnderlyingConn() == nil {
		return ""
	}
	return c.conn.UnderlyingConn().RemoteAddr().String()
}

func (c *wsConn) SendBuffer(buffer buffers.IBuffer) error {
	if buffer == nil {
		return kkerrors.ErrInvalidPacket
	}
	if err := kkpacket.DefaultStreamPacket().CheckPacketBuffer(buffer); err != nil {
		if c.stats != nil {
			c.stats.AddError()
		}
		kkbuffer.Put(buffer)
		return err
	}

	c.sendMu.Lock()
	// closing/wasEmpty/Push 必须在同一把锁里完成，否则会出现漏唤醒或 close 期间仍入队的竞态
	if c.closing.Load() {
		c.sendMu.Unlock()
		kkbuffer.Put(buffer)
		return kkerrors.ErrConnectionClosed
	}
	wasEmpty := c.sendQueue.IsEmpty()
	ok := c.sendQueue.Push(buffer)
	c.sendMu.Unlock()
	if !ok {
		if c.stats != nil {
			c.stats.AddError()
		}
		kkbuffer.Put(buffer)
		return kkerrors.ErrSendQueueFull
	}

	if wasEmpty { // 队列从空变为非空时才需要唤醒 writer，避免频繁唤醒
		c.wakeWriter()
	}

	return nil
}

func (c *wsConn) Close() error {
	c.closeWithError(nil, nil)
	return nil
}

func (c *wsConn) Context() context.Context {
	c.ctxMu.RLock()
	defer c.ctxMu.RUnlock()
	return c.ctx
}

func (c *wsConn) SetContext(ctx context.Context) {
	c.ctxMu.Lock()
	c.ctx = ctx
	c.ctxMu.Unlock()
}

func (c *wsConn) readLoop(dispatch func(kknet.IConn, buffers.IBuffer)) error {
	for {
		// Update read deadline if timeout is configured
		if c.opts.WsReadTimeout > 0 {
			if err := c.conn.SetReadDeadline(time.Now().Add(c.opts.WsReadTimeout)); err != nil {
				return err
			}
		}

		_, data, err := c.conn.ReadMessage()
		if err != nil {
			return err
		}

		dataLen := len(data)
		if c.stats != nil {
			c.stats.AddRecv(dataLen)
		}

		if dispatch != nil {
			packets := [writeBatchSize]buffers.IBuffer{}
			recvs, err := kkpacket.DefaultStreamPacket().Split(data, packets[:])
			if err != nil {
				if c.stats != nil {
					c.stats.AddError()
				}
				return err
			}
			for _, recv := range recvs {
				dispatch(c, recv)
			}
		}
	}
}

func (c *wsConn) drainSendQueueRelease() {
	if c.sendQueue == nil {
		return
	}

	for {
		c.sendMu.Lock()
		n := c.sendQueue.PopMany(writeBatchSize, c.batchBuffer[:])
		c.sendMu.Unlock()
		if n <= 0 {
			return
		}
		c.releaseBatch(n)
	}
}

func (c *wsConn) releaseBatch(n int) {
	for i := 0; i < n; i++ {
		bb := c.batchBuffer[i]
		c.batchBuffer[i] = nil
		if bb != nil {
			kkbuffer.Put(bb)
		}
	}
}

func (c *wsConn) writeLoop() {
	defer close(c.writeDone)

	batchBytes := make([]byte, 0, c.opts.WriteBufferSize)

	for {
		select {
		case <-c.wakeCh:
		case <-c.closeCh:
			// 快速退出：不再写出，只回收队列里的 buffer
			c.drainSendQueueRelease()
			return
		}

		for {
			// pop batch
			c.sendMu.Lock()
			n := c.sendQueue.PopMany(writeBatchSize, c.batchBuffer[:])
			remain := c.sendQueue.Len()
			closing := c.closing.Load()
			c.sendMu.Unlock()

			if n <= 0 {
				if closing && remain == 0 {
					c.signalDrained()
					return
				}
				break
			}

			c.writeMu.Lock()
			// Update write deadline if timeout is configured
			if c.opts.WsWriteTimeout > 0 {
				if err := c.conn.SetWriteDeadline(time.Now().Add(c.opts.WsWriteTimeout)); err != nil {
					if c.stats != nil {
						c.stats.AddError()
					}
					// 本轮已经 Pop 出来的 bb 必须回收，避免泄漏
					c.releaseBatch(n)
					c.writeMu.Unlock()
					c.drainSendQueueRelease()
					_ = c.conn.Close()
					return
				}
			}

			batchBytes = batchBytes[:0]
			for i := 0; i < n; i++ {
				bb := c.batchBuffer[i]
				c.batchBuffer[i] = nil
				if bb == nil {
					continue
				}
				batchBytes = append(batchBytes, bb.B...)
				kkbuffer.Put(bb)
			}
			var writeErr error
			if err := c.conn.WriteMessage(websocket.BinaryMessage, batchBytes); err != nil {
				writeErr = err
			}
			c.writeMu.Unlock()

			if writeErr != nil {
				if c.stats != nil {
					c.stats.AddError()
				}
				// 写失败：直接关闭底层连接，让 readLoop 触发 closeWithError(handler, err)
				c.drainSendQueueRelease()
				_ = c.conn.Close()
				return
			} else if c.stats != nil {
				c.stats.AddSent(len(batchBytes))
			}
		}
	}
}

func (c *wsConn) closeWithError(handler kknet.IHandler, err error) {
	c.closeOnce.Do(func() {
		c.closing.Store(true)

		// flush 模式：尽量把队列里的数据写完再关
		if c.opts.SendQueueNeedFlushOver && err == nil && c.drainedCh != nil {
			c.wakeWriter()
			timeout := c.opts.SendQueueTimeoutFlushOver
			if timeout <= 0 {
				timeout = c.opts.ShutdownTimeout
			}
			if timeout <= 0 {
				timeout = 10 * time.Second
			}
			select {
			case <-c.drainedCh:
			case <-time.After(timeout):
				if c.opts.SendQueueFlushTimeoutCallback != nil {
					c.opts.SendQueueFlushTimeoutCallback(c, timeout)
				}
				// 超时则强制停止 writer，并回收剩余 buffer
				c.stopWriter()
				select {
				case <-c.writeDone:
				case <-time.After(50 * time.Millisecond):
				}
			}
		} else {
			// 非 flush：立即停止 writer（不再写出，只回收）
			c.stopWriter()
			if c.writeDone != nil {
				select {
				case <-c.writeDone:
				case <-time.After(50 * time.Millisecond):
				}
			}
		}

		if c.stats != nil {
			c.stats.OnClose()
			if err != nil {
				c.stats.AddError()
			}
		}
		_ = c.conn.Close()
		if handler != nil {
			kknet.SafeHandlerCall(c.opts.Logger, c.stats, "kkws OnClose", func() {
				handler.OnClose(c, err)
			})
		}
	})
}
