package kktcp

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/panjf2000/gnet/v2"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

type tcpConn struct {
	id    kknet.CONN_ID
	conn  gnet.Conn
	opts  *kknet.Options
	stats *kknet.Stats

	ctxMu sync.RWMutex
	ctx   context.Context

	closing atomic.Bool

	rp kknet.IReadProcessor
	wp kknet.IWriteProcessor
}

var _ kknet.IConn = (*tcpConn)(nil)

func newTCPConn(c gnet.Conn, opts *kknet.Options, stats *kknet.Stats) *tcpConn {
	kknet.CheckOptions(opts)
	tc := &tcpConn{
		id:    kknet.NextConnID(),
		conn:  c,
		opts:  opts,
		stats: stats,
		ctx:   context.Background(),
	}

	if opts.RpProvider != nil {
		tc.rp = opts.RpProvider(opts.RpOptions)
	} else {
		tc.rp = defaultRpProvider(opts.RpOptions)
	}
	tc.rp.Start(tc)

	if opts.WpProvider != nil {
		tc.wp = opts.WpProvider(opts.WpOptions)
	} else {
		tc.wp = defaultWpProvider(opts.WpOptions)
	}
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

func (c *tcpConn) SendMsg(msg any) error {
	if msg == nil {
		return kkerrors.ErrInvalidPacket
	}
	if c.closing.Load() {
		return kkerrors.ErrConnectionClosed
	}
	if c.wp == nil {
		return kkerrors.ErrConnectionClosed
	}
	return c.wp.SendMsg(msg)
}

func (c *tcpConn) SendBuffer(buffer *kkbuffer.ByteBuffer) error {
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
