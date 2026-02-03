package kktcp

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/panjf2000/gnet/v2"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/netprocessor"
	"github.com/vvisun/kkdg/kknet/netprocessor/kkscsp"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

type tcpConn struct {
	id    kknet.CONN_ID
	conn  gnet.Conn
	opts  kknet.Options
	stats *kknet.Stats

	ctxMu sync.RWMutex
	ctx   context.Context

	closing atomic.Bool

	rp *kkscsp.ReadProcessor
	wp *kkscsp.WriteProcessor
}

var _ kknet.IConn = (*tcpConn)(nil)

func newTCPConn(c gnet.Conn, opts kknet.Options, stats *kknet.Stats) *tcpConn {
	kknet.CheckOptions(&opts)
	tc := &tcpConn{
		id:    kknet.NextConnID(),
		conn:  c,
		opts:  opts,
		stats: stats,
		ctx:   context.Background(),
	}
	tc.rp = kkscsp.NewReadProcessor(netprocessor.ReadOptions{
		RecvQueueSize:   opts.RecvQueueSize,
		RecvQueueStrict: opts.RecvQueueStrict,
		MsgHandler:      opts.MsgHandler,
		RawHandler:      opts.RawHandler,
	})
	tc.rp.Start(tc)

	tc.wp = kkscsp.NewWriteProcessor(netprocessor.WriteOptions{
		SendQueueSize:                 opts.SendQueueSize,
		SendQueueStrict:               opts.SendQueueStrict,
		SendQueueNeedFlushOver:        opts.SendQueueNeedFlushOver,
		SendQueueTimeoutFlushOver:     opts.SendQueueTimeoutFlushOver,
		SendQueueFlushTimeoutCallback: opts.SendQueueFlushTimeoutCallback,
		WriteBatchSize:                opts.WriteBatchSize,
		WriteBatchLimitBytes:          opts.WriteBatchLimitBytes,
	})
	tc.wp.Start(tc, tc.writeBatch, func(_ error) {
		_ = tc.conn.Close()
	})
	return tc
}

func (c *tcpConn) ID() kknet.CONN_ID {
	return c.id
}

func (c *tcpConn) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}

func (c *tcpConn) Close() error {
	c.closing.Store(true)
	if c.wp != nil {
		go c.wp.Stop(kkerrors.ErrConnectionClosed)
	}
	if c.rp != nil {
		go c.rp.Stop()
	}
	return c.conn.Close()
}

func (c *tcpConn) Context() context.Context {
	c.ctxMu.RLock()
	defer c.ctxMu.RUnlock()
	return c.ctx
}

func (c *tcpConn) SetContext(ctx context.Context) {
	c.ctxMu.Lock()
	c.ctx = ctx
	c.ctxMu.Unlock()
}

func (c *tcpConn) SendBuffer(buffer buffers.IBuffer) error {
	if c.closing.Load() {
		kkbuffer.Put(buffer)
		return kkerrors.ErrConnectionClosed
	}

	if err := kkpacket.DefaultStreamPacket().CheckPacketBuffer(buffer); err != nil {
		if c.stats != nil {
			c.stats.AddError()
		}
		kkbuffer.Put(buffer)
		return err
	}

	if c.wp == nil {
		kkbuffer.Put(buffer)
		return kkerrors.ErrConnectionClosed
	}
	if err := c.wp.SendBuffer(buffer); err != nil {
		if c.stats != nil {
			c.stats.AddError()
		}
		return err
	}
	return nil
}

/*
*批量写入。WriteFunc中，发送失败的数据不释放，供调用方知道哪些数据发送失败。
 *@param batch 批量缓冲区，数组长度为 WriteOptions.WriteBatchSize
 *@param n 批量数量
 *@return error
*/
func (c *tcpConn) writeBatch(batch []*kkbuffer.ByteBuffer, n int) error {
	for i := 0; i < n; i++ {
		bb := batch[i]
		if bb == nil {
			continue
		}
		// NOTE: AsyncWrite is safe across goroutines; keep bb until callback.
		if err := c.conn.AsyncWrite(bb.B, func(_ gnet.Conn, err error) error {
			if err != nil {
				if c.stats != nil {
					c.stats.AddError()
				}
			} else if c.stats != nil {
				c.stats.AddSent(len(bb.B))
			}
			kkbuffer.Put(bb)
			return nil
		}); err != nil {
			//发送失败，保持剩余数据在批量中，供调用方知道哪些数据发送失败。
			if c.stats != nil {
				c.stats.AddError()
			}
			return err
		}
		batch[i] = nil // 成功才释放。
	}
	return nil
}
