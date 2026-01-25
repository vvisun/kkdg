package kktcp

import (
	"context"
	"sync"

	"github.com/panjf2000/gnet/v2"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
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
}

var _ kknet.IConn = (*gnetClientConn)(nil)

func newGnetClientConn(c gnet.Conn, opts kknet.Options, stats *kknet.Stats) *gnetClientConn {
	return &gnetClientConn{
		id:    kknet.NextConnID(),
		conn:  c,
		opts:  opts,
		stats: stats,
		ctx:   context.Background(),
	}
}

func (c *gnetClientConn) ID() kknet.CONN_ID {
	return c.id
}

func (c *gnetClientConn) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}

func (c *gnetClientConn) SendBuffer(buffer buffers.IBuffer) error {
	if err := kkpacket.DefaultStreamPacket().CheckPacket(buffer.B); err != nil {
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

func (c *gnetClientConn) Send(data []byte) error {
	if err := kkpacket.DefaultStreamPacket().CheckPacket(data); err != nil {
		if c.stats != nil {
			c.stats.AddError()
		}
		return err
	}
	bb := kkbuffer.GetWithCapacity(len(data))
	bb.B = bb.B[:len(data)]
	copy(bb.B, data)
	return c.SendBuffer(bb)
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
