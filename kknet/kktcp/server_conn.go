package kktcp

import (
	"context"
	"errors"
	"sync"

	"github.com/panjf2000/gnet/v2"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
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
}

var _ kknet.IConn = (*tcpConn)(nil)

func newTCPConn(c gnet.Conn, opts kknet.Options, stats *kknet.Stats) *tcpConn {
	return &tcpConn{
		id:    kknet.NextConnID(),
		conn:  c,
		opts:  opts,
		stats: stats,
		ctx:   context.Background(),
	}
}

func (c *tcpConn) ID() kknet.CONN_ID {
	return c.id
}

func (c *tcpConn) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}

func (c *tcpConn) SendBuffer(buffer buffers.IBuffer) error {
	// todo:
	return errors.New("not implemented")
}

func (c *tcpConn) Send(data []byte) error {
	if len(data) > kkpacket.DefaultMaxMessageSize() {
		if c.stats != nil {
			c.stats.AddError()
		}
		return kkerrors.ErrMaxMessageSize
	}

	bb, err1 := c.opts.StreamPacket.Pack(data, kkpacket.DefaultMaxMessageSize())
	if err1 != nil {
		kkbuffer.Put(bb)
		if c.stats != nil {
			c.stats.AddError()
		}
		return err1
	}

	err := c.conn.AsyncWrite(bb.B, func(_ gnet.Conn, _ error) error {
		kkbuffer.Put(bb)
		return nil
	})
	if err != nil {
		kkbuffer.Put(bb)
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
