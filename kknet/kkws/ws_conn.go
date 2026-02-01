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
	"github.com/vvisun/kkdg/kknet/netprocessor"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

const writeBatchSize = 32 // 每轮持锁时最多 Pop 的帧数，减少 Lock 次数与 Send 竞争

type wsConn struct {
	id    kknet.CONN_ID
	conn  *websocket.Conn
	opts  kknet.Options
	stats *kknet.Stats
	ctxMu sync.RWMutex
	ctx   context.Context

	closeOnce sync.Once
	closing   atomic.Bool

	writeMu sync.Mutex // websocket 写必须串行

	wp        *netprocessor.WriteProcessor
	writeDone <-chan struct{} // 兼容测试：writer 退出信号
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
	size := c.opts.SendQueueSize
	if size <= 0 {
		size = kknet.DefaultOptions().SendQueueSize
	}
	limitBytes := c.opts.WriteBufferSize
	if limitBytes > 2048 {
		limitBytes = 2048
	}
	wp := netprocessor.NewWriteProcessor(netprocessor.WriteOptions{
		SendQueueSize:                 size,
		SendQueueStrict:               c.opts.SendQueueStrict,
		SendQueueNeedFlushOver:        c.opts.SendQueueNeedFlushOver,
		SendQueueTimeoutFlushOver:     c.opts.SendQueueTimeoutFlushOver,
		SendQueueFlushTimeoutCallback: c.opts.SendQueueFlushTimeoutCallback,
		WriteBatchSize:                writeBatchSize,
		WriteBatchLimitBytes:          limitBytes,
	})
	c.wp = wp
	c.writeDone = wp.Done()

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

func (c *wsConn) SendBufferSync(buffer buffers.IBuffer) error {
	return c.SendBuffer(buffer)
}

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

func (c *wsConn) readLoop() error {
	rp := netprocessor.NewReadProcessor(netprocessor.ReadOptions{
		RecvQueueSize:   c.opts.RecvQueueSize,
		RecvQueueStrict: c.opts.RecvQueueStrict,
		MsgHandler:      c.opts.MsgHandler,
		RawHandler:      c.opts.RawHandler,
	})
	rp.Start(c)
	defer rp.Stop()

	for {
		// Update read deadline if timeout is configured
		if c.opts.WsReadTimeout > 0 {
			if err := c.conn.SetReadDeadline(time.Now().Add(c.opts.WsReadTimeout)); err != nil {
				return err
			}
		}

		_, data, err := c.conn.ReadMessage()
		if err != nil {
			// ensure all queued packets are processed before returning
			rp.Stop()
			return err
		}

		dataLen := len(data)
		if c.stats != nil {
			c.stats.AddRecv(dataLen)
		}

		if err := rp.OnRecvBytes(data); err != nil {
			if c.stats != nil {
				c.stats.AddError()
			}
			rp.Stop()
			return err
		}
	}
}

func (c *wsConn) writeBatch(batch []*kkbuffer.ByteBuffer, n int) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	// Update write deadline if timeout is configured
	if c.opts.WsWriteTimeout > 0 {
		if err := c.conn.SetWriteDeadline(time.Now().Add(c.opts.WsWriteTimeout)); err != nil {
			if c.stats != nil {
				c.stats.AddError()
			}
			// caller (write processor) will release buffers
			return err
		}
	}

	batchBytes := make([]byte, 0, c.opts.WriteBufferSize)
	for i := 0; i < n; i++ {
		bb := batch[i]
		batch[i] = nil
		if bb == nil {
			continue
		}
		batchBytes = append(batchBytes, bb.B...)
		kkbuffer.Put(bb)
	}

	if err := c.conn.WriteMessage(websocket.BinaryMessage, batchBytes); err != nil {
		if c.stats != nil {
			c.stats.AddError()
		}
		return err
	}
	if c.stats != nil {
		c.stats.AddSent(len(batchBytes))
	}
	return nil
}

func (c *wsConn) closeWithError(handler kknet.INewHandler, err error) {
	c.closeOnce.Do(func() {
		c.closing.Store(true)
		if c.wp != nil {
			c.wp.Stop(err)
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
