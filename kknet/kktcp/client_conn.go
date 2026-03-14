package kktcp

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/panjf2000/gnet/v2"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

type gnetClientConn struct {
	id    kknet.CONN_ID
	conn  gnet.Conn
	opts  *kknet.Options
	stats *kknet.Stats

	closing atomic.Bool

	rp kknet.IReadProcessor

	// 跟踪待发送的 AsyncWrite，Close 时等待其完成
	pendingWrites atomic.Int32
	closeMu       sync.Mutex
	closeCond     *sync.Cond

	// 自定义数据
	extraMu   sync.RWMutex
	extraData any
}

var _ kknet.IConn = (*gnetClientConn)(nil)

func newGnetClientConn(c gnet.Conn, opts *kknet.Options, stats *kknet.Stats) *gnetClientConn {
	kknet.CheckOptions(opts)
	cc := &gnetClientConn{
		id:    kknet.NextConnID(),
		conn:  c,
		opts:  opts,
		stats: stats,
	}
	cc.closeCond = sync.NewCond(&cc.closeMu)
	if opts.RpProvider != nil {
		cc.rp = opts.RpProvider(opts.RpOptions)
	} else {
		cc.rp = defaultRpProvider(opts.RpOptions)
	}
	cc.rp.Start(cc)

	return cc
}

func (c *gnetClientConn) ID() kknet.CONN_ID {
	return c.id
}

func (c *gnetClientConn) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}

func (c *gnetClientConn) SetExtraData(extraData any) {
	c.extraMu.Lock()
	c.extraData = extraData
	c.extraMu.Unlock()
}

func (c *gnetClientConn) GetExtraData() any {
	c.extraMu.RLock()
	data := c.extraData
	c.extraMu.RUnlock()
	return data
}

func (c *gnetClientConn) Close() error {
	if c.closing.Swap(true) {
		return nil
	}
	if c.rp != nil {
		go c.rp.Stop()
	}
	if c.opts.WpOptions.SendQueueNeedFlushOver {
		// 需要 flush：等待所有待发送的 AsyncWrite 完成（带超时）
		timeout := c.opts.WpOptions.SendQueueTimeoutFlushOver
		if timeout <= 0 {
			timeout = 10 * time.Second
		}
		done := make(chan struct{})
		go func() {
			c.closeMu.Lock()
			for c.pendingWrites.Load() > 0 {
				c.closeCond.Wait()
			}
			c.closeMu.Unlock()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(timeout):
			if c.opts.WpOptions.SendQueueFlushTimeoutCallback != nil {
				c.opts.WpOptions.SendQueueFlushTimeoutCallback(c, timeout)
			}
		}
	}
	return c.conn.Close()
}

func (c *gnetClientConn) SendMsg(msg any) error {
	if msg == nil {
		return kkerrors.ErrClusterInvalidPacket
	}
	if c.closing.Load() {
		return kkerrors.ErrNetConnectionClosed
	}
	buffer, err := kkpacket.EncodeStream(msg, c.opts.StreamTool, c.opts.WpOptions.MsgPacket)
	if err != nil {
		kkbuffer.Put(buffer)
		return err
	}
	return c.SendBuffer(buffer)
}

func (c *gnetClientConn) SendBuffer(buffer *kkbuffer.ByteBuffer) error {
	if err := c.opts.StreamTool.CheckPacketBuffer(buffer); err != nil {
		if c.stats != nil {
			c.stats.AddError()
		}
		kkbuffer.Put(buffer)
		return err
	}
	if c.closing.Load() {
		kkbuffer.Put(buffer)
		return kkerrors.ErrNetConnectionClosed
	}

	c.pendingWrites.Add(1)
	err := c.conn.AsyncWrite(buffer.B, func(_ gnet.Conn, err error) error {
		if err != nil {
			if c.stats != nil {
				c.stats.AddError()
			}
			kkbuffer.Put(buffer)
		} else {
			if c.stats != nil {
				c.stats.AddSent(len(buffer.B))
			}
			kkbuffer.Put(buffer)
		}
		if c.pendingWrites.Add(-1) == 0 {
			c.closeMu.Lock()
			c.closeCond.Signal()
			c.closeMu.Unlock()
		}
		return nil
	})
	if err != nil {
		c.pendingWrites.Add(-1)
		if c.pendingWrites.Load() == 0 {
			c.closeMu.Lock()
			c.closeCond.Signal()
			c.closeMu.Unlock()
		}
		return err
	}
	return nil
}
