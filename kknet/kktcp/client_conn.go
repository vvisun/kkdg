package kktcp

import (
	"context"
	"sync"

	"github.com/panjf2000/gnet/v2"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/netprocessor"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

type gnetClientConn struct {
	id    kknet.CONN_ID
	conn  gnet.Conn
	opts  kknet.Options
	stats *kknet.Stats

	ctxMu sync.RWMutex
	ctx   context.Context

	rp *netprocessor.ReadProcessor
}

var _ kknet.IConn = (*gnetClientConn)(nil)

func newGnetClientConn(c gnet.Conn, opts kknet.Options, stats *kknet.Stats) *gnetClientConn {
	cc := &gnetClientConn{
		id:    kknet.NextConnID(),
		conn:  c,
		opts:  opts,
		stats: stats,
		ctx:   context.Background(),
	}
	cc.rp = netprocessor.NewReadProcessor(netprocessor.ReadOptions{
		RecvQueueSize:   opts.RecvQueueSize,
		RecvQueueStrict: opts.RecvQueueStrict,
		MsgHandler:      opts.MsgHandler,
		RawHandler:      opts.RawHandler,
	})
	cc.rp.Start(cc)
	return cc
}

func (c *gnetClientConn) ID() kknet.CONN_ID {
	return c.id
}

func (c *gnetClientConn) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}

func (c *gnetClientConn) SendBuffer(buffer buffers.IBuffer) error {
	if err := kkpacket.DefaultStreamPacket().CheckPacketBuffer(buffer); err != nil {
		if c.stats != nil {
			c.stats.AddError()
		}
		return err
	}
	old := buffer
	bb := kkbuffer.GetWithCapacity(len(old.B))
	bb.B, old.B = old.B, bb.B
	kkbuffer.Put(old)

	// TODO: 发送失败应该入队，下次优先从队列中取数据发送。
	if err := c.conn.AsyncWrite(bb.B, func(_ gnet.Conn, err error) error {
		if err != nil {
			// 发送失败
			if c.stats != nil {
				c.stats.AddError()
			}
		} else if c.stats != nil {
			c.stats.AddSent(len(bb.B))
		}
		kkbuffer.Put(bb)
		return nil
	}); err != nil {
		// 入队失败
		kkbuffer.Put(bb)
		if c.stats != nil {
			c.stats.AddError()
		}
		return err
	}
	// 入队成功立即返回，注意这里只是入队，并非真正的发送数据
	return nil
}

func (c *gnetClientConn) Close() error {
	return c.conn.Close()
}

func (c *gnetClientConn) Context() context.Context {
	c.ctxMu.RLock()
	defer c.ctxMu.RUnlock()
	return c.ctx
}

func (c *gnetClientConn) SetContext(ctx context.Context) {
	c.ctxMu.Lock()
	c.ctx = ctx
	c.ctxMu.Unlock()
}
