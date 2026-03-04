package kktcp

import (
	"sync/atomic"

	"github.com/panjf2000/gnet/v2"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kkprocessor"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

type tcpConn struct {
	id    kknet.CONN_ID
	uid   kknet.USER_ID
	conn  gnet.Conn
	opts  *kknet.Options
	stats *kknet.Stats

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
		tc.wp = kkprocessor.NewWriteProcessor(opts.WpOptions)
	}
	tc.wp.Start(tc, tc.writeBatch, func(err error) {
		if stats != nil {
			stats.AddError()
		}
	})

	return tc
}

func (c *tcpConn) ID() kknet.CONN_ID {
	return c.id
}

func (c *tcpConn) BindUser(uid kknet.USER_ID) {
	c.uid = uid
}

func (c *tcpConn) UnbindUser() {
	c.uid = kknet.NULL_USER_ID
}

func (c *tcpConn) GetUserId() kknet.USER_ID {
	return c.uid
}

func (c *tcpConn) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}

func (c *tcpConn) Close() error {
	c.closing.Store(true)
	if c.rp != nil {
		go c.rp.Stop()
	}
	if c.wp != nil {
		c.wp.Stop(nil)
	}
	return c.conn.Close()
}

func (c *tcpConn) SendMsg(msg any) error {
	if msg == nil {
		return kkerrors.ErrInvalidPacket
	}
	if c.closing.Load() {
		return kkerrors.ErrConnectionClosed
	}
	buffer, err := kkpacket.EncodeStream(msg, kkpacket.DefaultStreamPacket(), c.opts.WpOptions.MsgPacket)
	if err != nil {
		kkbuffer.Put(buffer)
		return err
	}
	return c.SendBuffer(buffer)
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
	if n <= 0 {
		return nil
	}
	if c.conn == nil {
		return kkerrors.ErrConnectionClosed
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
