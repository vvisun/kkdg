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

type gnetClientConn struct {
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

var _ kknet.IConn = (*gnetClientConn)(nil)

func newGnetClientConn(c gnet.Conn, opts kknet.Options, stats *kknet.Stats) *gnetClientConn {
	kknet.CheckOptions(&opts)
	cc := &gnetClientConn{
		id:    kknet.NextConnID(),
		conn:  c,
		opts:  opts,
		stats: stats,
		ctx:   context.Background(),
	}
	cc.rp = kkscsp.NewReadProcessor(netprocessor.ReadOptions{
		RecvQueueSize:   opts.RecvQueueSize,
		RecvQueueStrict: opts.RecvQueueStrict,
		MsgHandler:      opts.MsgHandler,
		RawHandler:      opts.RawHandler,
	})
	cc.rp.Start(cc)

	cc.wp = kkscsp.NewWriteProcessor(netprocessor.WriteOptions{
		SendQueueSize:                 opts.SendQueueSize,
		SendQueueStrict:               opts.SendQueueStrict,
		SendQueueNeedFlushOver:        opts.SendQueueNeedFlushOver,
		SendQueueTimeoutFlushOver:     opts.SendQueueTimeoutFlushOver,
		SendQueueFlushTimeoutCallback: opts.SendQueueFlushTimeoutCallback,
		WriteBatchSize:                opts.WriteBatchSize,
		WriteBatchLimitBytes:          opts.WriteBatchLimitBytes,
	})
	cc.wp.Start(cc, cc.writeBatch, func(_ error) {
		_ = cc.conn.Close()
	})
	return cc
}

func (c *gnetClientConn) ID() kknet.CONN_ID {
	return c.id
}

func (c *gnetClientConn) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
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

func (c *gnetClientConn) Close() error {
	c.closing.Store(true)
	if c.wp != nil {
		go c.wp.Stop(kkerrors.ErrConnectionClosed)
	}
	if c.rp != nil {
		go c.rp.Stop()
	}
	return c.conn.Close()
}

func (c *gnetClientConn) SendBuffer(buffer buffers.IBuffer) error {
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

// writeBatch 写入批量数据, return failList, error
func (c *gnetClientConn) writeBatch(batch []*kkbuffer.ByteBuffer, n int) ([]*kkbuffer.ByteBuffer, error) {
	succCnt := 0

	for i := 0; i < n; i++ {
		bb := batch[i]
		batch[i] = nil
		if bb == nil {
			continue
		}
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
			kkbuffer.Put(bb)
			if c.stats != nil {
				c.stats.AddError()
			}
			fails := batch[succCnt:n]
			return fails, err
		}
		succCnt = i + 1
	}
	return nil, nil
}
