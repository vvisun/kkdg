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

type tcpConn struct {
	id    kknet.CONN_ID
	conn  gnet.Conn
	opts  kknet.Options
	stats *kknet.Stats

	ctxMu sync.RWMutex
	ctx   context.Context

	rp *netprocessor.ReadProcessor
}

var _ kknet.IConn = (*tcpConn)(nil)

func newTCPConn(c gnet.Conn, opts kknet.Options, stats *kknet.Stats) *tcpConn {
	tc := &tcpConn{
		id:    kknet.NextConnID(),
		conn:  c,
		opts:  opts,
		stats: stats,
		ctx:   context.Background(),
	}
	tc.rp = netprocessor.NewReadProcessor(netprocessor.ReadOptions{
		RecvQueueSize:   opts.RecvQueueSize,
		RecvQueueStrict: opts.RecvQueueStrict,
		MsgHandler:      opts.MsgHandler,
		RawHandler:      opts.RawHandler,
	})
	tc.rp.Start(tc)
	return tc
}

func (c *tcpConn) ID() kknet.CONN_ID {
	return c.id
}

func (c *tcpConn) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}

func (c *tcpConn) SendBuffer(buffer buffers.IBuffer) error {
	if err := kkpacket.DefaultStreamPacket().CheckPacketBuffer(buffer); err != nil {
		if c.stats != nil {
			c.stats.AddError()
		}
		return err
	}

	bb := buffer

	// TODO: 发送失败应该入队，下次优先从队列中取数据发送。
	err := c.conn.AsyncWrite(bb.B, func(_ gnet.Conn, err error) error {
		if err != nil {
			// 发送失败
			if c.stats != nil {
				c.stats.AddError()
			}
		} else if c.stats != nil {
			// 只在真正写成功时统计 sent，避免入队成功但发送失败造成统计偏差
			c.stats.AddSent(len(bb.B))
		}
		kkbuffer.Put(bb)
		return nil
	})
	if err != nil {
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

func (c *tcpConn) Close() error {
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
