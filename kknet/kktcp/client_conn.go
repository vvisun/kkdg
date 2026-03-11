package kktcp

import (
	"sync/atomic"

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
	wp kknet.IWriteProcessor

	extraData any // 自定义数据
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
	if opts.RpProvider != nil {
		cc.rp = opts.RpProvider(opts.RpOptions)
	} else {
		cc.rp = defaultRpProvider(opts.RpOptions)
	}
	cc.rp.Start(cc)

	if opts.WpProvider != nil {
		cc.wp = opts.WpProvider(opts.WpOptions)
	} else {
		cc.wp = defaultWpProvider(opts.WpOptions)
	}
	cc.wp.Start(cc, cc.writeBatch, func(err error) {
		if stats != nil {
			stats.AddError()
		}
	})

	return cc
}

func (c *gnetClientConn) ID() kknet.CONN_ID {
	return c.id
}

func (c *gnetClientConn) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}

func (c *gnetClientConn) SetExtraData(extraData any) {
	c.extraData = extraData
}

func (c *gnetClientConn) GetExtraData() any {
	return c.extraData
}

func (c *gnetClientConn) Close() error {
	c.closing.Store(true)
	if c.rp != nil {
		go c.rp.Stop()
	}
	if c.wp != nil {
		c.wp.Stop(nil)
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
	buffer, err := kkpacket.EncodeStream(msg, kkpacket.DefaultStreamPacket(), c.opts.WpOptions.MsgPacket)
	if err != nil {
		kkbuffer.Put(buffer)
		return err
	}
	return c.SendBuffer(buffer)
}

func (c *gnetClientConn) SendBuffer(buffer *kkbuffer.ByteBuffer) error {
	if err := kkpacket.DefaultStreamPacket().CheckPacketBuffer(buffer); err != nil {
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
	if c.wp == nil {
		kkbuffer.Put(buffer)
		return kkerrors.ErrNetConnectionClosed
	}
	return c.wp.SendBuffer(buffer)
}

/*
*批量写入。WriteFunc中，发送失败的数据不释放，供调用方知道哪些数据发送失败。
 *@param batch 批量缓冲区，数组长度为 WriteOptions.WriteBatchSize
 *@param n 批量数量
 *@return error
*/
func (c *gnetClientConn) writeBatch(batch []*kkbuffer.ByteBuffer, n int) error {
	if n <= 0 {
		return nil
	}
	if c.conn == nil {
		return kkerrors.ErrNetConnectionClosed
	}

	if n > 1 {
		bs := make([][]byte, n)
		totalBytes := 0
		for i := 0; i < n; i++ {
			bs[i] = batch[i].B
			totalBytes += len(bs[i])
		}
		done := make(chan struct{})
		var writeErr error
		err := c.conn.AsyncWritev(bs, func(_ gnet.Conn, err error) error {
			if err != nil {
				if c.stats != nil {
					c.stats.AddError()
				}
				writeErr = err
			} else {
				if c.stats != nil {
					c.stats.AddSent(totalBytes)
				}
				for j := 0; j < n; j++ {
					kkbuffer.Put(batch[j])
					batch[j] = nil
				}
			}
			close(done)
			return nil
		})
		if err != nil {
			if c.stats != nil {
				c.stats.AddError()
			}
			return err
		}
		<-done
		return writeErr
	}

	bb := batch[0]
	if bb == nil {
		return nil
	}
	done := make(chan struct{})
	var writeErr error
	err := c.conn.AsyncWrite(bb.B, func(_ gnet.Conn, err error) error {
		if err != nil {
			if c.stats != nil {
				c.stats.AddError()
			}
			writeErr = err
		} else {
			if c.stats != nil {
				c.stats.AddSent(len(bb.B))
			}
			kkbuffer.Put(bb)
			batch[0] = nil
		}
		close(done)
		return nil
	})
	if err != nil {
		if c.stats != nil {
			c.stats.AddError()
		}
		return err
	}
	<-done
	return writeErr
}
