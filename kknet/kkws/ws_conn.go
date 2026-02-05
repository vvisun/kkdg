package kkws

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/gorilla/websocket"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/netprocessor"
	"github.com/vvisun/kkdg/kknet/netprocessor/kkscsp"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/timingwheel"
)

type wsConn struct {
	id    kknet.CONN_ID
	conn  *websocket.Conn
	opts  *kknet.Options // 配置
	stats *kknet.Stats   // 统计信息
	ctxMu sync.RWMutex
	ctx   context.Context

	closeOnce sync.Once
	closing   atomic.Bool // 连接关闭标志

	writeMu sync.Mutex // websocket 写必须串行

	wp              netprocessor.IWriteProcessor // 写处理器
	batchWriteBuf   []byte                       // 批量写入缓冲区
	batchWriteLimit int                          // 批量写入限制字节数

	readBB *kkbuffer.ByteBuffer // reused read buffer for NextReader

	// ping 由时间轮调度，关闭连接时需 Stop 取消
	pingTimer unsafe.Pointer // *timingwheel.Timer
}

var _ kknet.IConn = (*wsConn)(nil)

func newWSConn(conn *websocket.Conn, opts *kknet.Options, stats *kknet.Stats) *wsConn {
	kknet.CheckOptions(opts)
	c := &wsConn{
		id:              kknet.NextConnID(),
		conn:            conn,
		opts:            opts,
		stats:           stats,
		ctx:             context.Background(),
		batchWriteBuf:   make([]byte, 0, opts.WpOptions.WriteBatchLimitBytes),
		batchWriteLimit: opts.WpOptions.WriteBatchLimitBytes,
	}
	c.initSendQueue()
	return c
}

func (c *wsConn) initSendQueue() {
	wp := kkscsp.NewWriteProcessor(c.opts.WpOptions)
	c.wp = wp

	wp.Start(c, c.writeBatch, func(_ error) {
		// close underlying conn to force readLoop to exit
		_ = c.conn.Close()
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

// wsPingScheduler 用于时间轮按 WsPingInterval 周期触发 Ping。
type wsPingScheduler struct {
	interval time.Duration
}

func (s *wsPingScheduler) Next(prev time.Time) time.Time {
	return prev.Add(s.interval)
}

// startPingByTimingWheel 使用时间轮按 WsPingInterval 发送 Ping；收到 Pong 由 SetPongHandler 刷新读超时。
func (c *wsConn) startPingByTimingWheel() {
	if c.opts.PingInterval <= 0 || c.opts.ReadTimeout <= 0 {
		return
	}

	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(c.opts.ReadTimeout))
	})

	tw := timingwheel.GetNetTimingWheel()
	t := tw.ScheduleFunc(&wsPingScheduler{c.opts.PingInterval}, func() {
		if c.closing.Load() {
			return
		}
		if c.conn == nil {
			return
		}
		deadline := time.Now().Add(c.opts.PingInterval * 2)
		if err := c.conn.WriteControl(websocket.PingMessage, nil, deadline); err != nil {
			c.opts.Logger.Errorf("kkws write ping message error: %v", err)
			return
		}
	})
	if t != nil {
		atomic.StorePointer(&c.pingTimer, unsafe.Pointer(t))
	}
}

func (c *wsConn) stopPingByTimingWheel() {
	if p := atomic.LoadPointer(&c.pingTimer); p != nil {
		(*timingwheel.Timer)(p).Stop()
		atomic.StorePointer(&c.pingTimer, nil)
	}
}

func (c *wsConn) closeWithError(handler kknet.IConnLifecycleHandler, err error) {
	c.closeOnce.Do(func() {
		c.closing.Store(true)
		c.stopPingByTimingWheel()
		if c.wp != nil {
			c.wp.Stop(err)
		}
		if c.readBB != nil {
			kkbuffer.Put(c.readBB)
			c.readBB = nil
		}

		if c.stats != nil {
			c.stats.OnClose()
			if err != nil {
				c.opts.Logger.Debugf("kkws OnClose error: connId=%d, err=%v", c.id, err)
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

func (c *wsConn) readLoop() error {
	rp := kkscsp.NewReadProcessor(c.opts.RpOptions)
	rp.Start(c)
	defer rp.Stop()

	// Ping/Pong keepalive: 用时间轮按间隔发 Ping；收到 Pong 刷新读超时。
	c.startPingByTimingWheel()

	for {
		// Update read deadline if timeout is configured
		if c.opts.ReadTimeout > 0 {
			if err := c.conn.SetReadDeadline(time.Now().Add(c.opts.ReadTimeout)); err != nil {
				return err
			}
		}

		mt, r, err := c.conn.NextReader()
		if err != nil {
			// ensure all queued packets are processed before returning
			rp.Stop()
			return err
		}

		if mt != websocket.BinaryMessage {
			// Drain non-binary message and ignore.
			_, _ = io.Copy(io.Discard, r)
			continue
		}

		if c.readBB == nil {
			// start with ReadBufferSize to reduce early grows; it will grow if needed.
			c.readBB = kkbuffer.GetWithCapacity(c.opts.ReadBufferSize)
		}
		c.readBB.Reset()
		n64, err := c.readBB.ReadFrom(r)
		if err != nil {
			rp.Stop()
			return err
		}
		dataLen := int(n64)
		if c.stats != nil {
			c.stats.AddRecv(dataLen)
		}

		if err := rp.OnRecvBytes(c.readBB.B); err != nil {
			if c.stats != nil {
				c.stats.AddError()
			}
			rp.Stop()
			return err
		}
	}
}

// SendBuffer 异步发送数据。
func (c *wsConn) SendBuffer(buffer buffers.IBuffer) error {
	if err := kkpacket.DefaultStreamPacket().CheckPacketBuffer(buffer); err != nil {
		if c.stats != nil {
			c.stats.AddError()
		}
		kkbuffer.Put(buffer)
		return err
	}

	if c.closing.Load() {
		kkbuffer.Put(buffer)
		return kkerrors.ErrConnectionClosed
	}
	if c.wp == nil {
		kkbuffer.Put(buffer)
		return kkerrors.ErrConnectionClosed
	}
	return c.wp.SendBuffer(buffer)
}

/*
*批量写入。WriteFunc中，发送失败的数据不释放，供调用方知道哪些数据发送失败。
 *@param batch 批量缓冲区，数组长度为 WriteOptions.WriteBatchSize
 *@param n 批量数量
 *@return error
*/
func (c *wsConn) writeBatch(batch []*kkbuffer.ByteBuffer, n int) error {
	if n <= 0 {
		return nil
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	// Update write deadline if timeout is configured
	if c.opts.WriteTimeout > 0 {
		if err := c.conn.SetWriteDeadline(time.Now().Add(c.opts.WriteTimeout)); err != nil {
			if c.stats != nil {
				c.stats.AddError()
			}
			// caller (write processor) will release buffers
			return err
		}
	}

	batchBytes := c.batchWriteBuf[:0]
	start := 0 // start index of current batchBytes ownership window
	for i := 0; i < n; i++ {
		bb := batch[i]
		if bb == nil {
			continue
		}
		batchBytes = append(batchBytes, bb.B...)
		if len(batchBytes) >= c.batchWriteLimit {
			// 单次写入超过限制，则立即发送
			if err := c.sendBytes(batchBytes); err != nil {
				// 发送失败，保持剩余数据在批量中，供调用方知道哪些数据发送失败。
				return err
			}
			batchBytes = batchBytes[:0]
			// 发送成功，释放已发送的数据。
			for j := start; j <= i; j++ {
				bb2 := batch[j]
				batch[j] = nil
				if bb2 != nil {
					kkbuffer.Put(bb2)
				}
			}
			start = i + 1
		}
	}

	// 发送剩余数据
	if len(batchBytes) > 0 {
		if err := c.sendBytes(batchBytes); err != nil {
			// 发送失败，保持剩余数据在批量中，供调用方知道哪些数据发送失败。
			return err
		}
	}

	// 发送成功，释放剩余数据。
	for j := start; j < n; j++ {
		bb := batch[j]
		batch[j] = nil
		if bb != nil {
			kkbuffer.Put(bb)
		}
	}
	return nil
}

// sendBytes 发送字节数据
func (c *wsConn) sendBytes(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	err := c.conn.WriteMessage(websocket.BinaryMessage, data)
	if err != nil {
		if c.stats != nil {
			c.stats.AddError()
		}
		return err
	}
	if c.stats != nil {
		c.stats.AddSent(len(data))
	}
	return nil
}
