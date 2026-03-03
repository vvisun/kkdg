package kktcp

import (
	"sync"
	"sync/atomic"

	"github.com/panjf2000/gnet/v2"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/queues/bbqueue"
)

type tcpConn struct {
	id    kknet.CONN_ID
	uid   kknet.USER_ID
	conn  gnet.Conn
	opts  *kknet.Options
	stats *kknet.Stats

	closing atomic.Bool

	rp kknet.IReadProcessor

	sendQueue bbqueue.IFiFoQueue //发送队列
	sendMu    sync.Mutex
}

var _ kknet.IConn = (*tcpConn)(nil)

func newTCPConn(c gnet.Conn, opts *kknet.Options, stats *kknet.Stats) *tcpConn {
	kknet.CheckOptions(opts)
	tc := &tcpConn{
		id:        kknet.NextConnID(),
		conn:      c,
		opts:      opts,
		stats:     stats,
		sendQueue: bbqueue.NewFIFOQueue(opts.WpOptions.SendQueueSize, opts.WpOptions.SendQueueStrict),
	}

	if opts.RpProvider != nil {
		tc.rp = opts.RpProvider(opts.RpOptions)
	} else {
		tc.rp = defaultRpProvider(opts.RpOptions)
	}
	tc.rp.Start(tc)

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

	c.sendMu.Lock()
	ok := c.sendQueue.Push(buffer)
	c.sendMu.Unlock()
	if !ok {
		kkbuffer.Put(buffer)
		return kkerrors.ErrSendQueueFull
	}

	return c.doWrite()
}

func (c *tcpConn) doWrite() error {
	batchArr := [64]*kkbuffer.ByteBuffer{}
	sendBatchBuffer := batchArr[:]
	sbbLen := len(sendBatchBuffer)
	c.sendMu.Lock()
	n := c.sendQueue.PopMany(sbbLen, sendBatchBuffer, c.opts.WpOptions.BatchWriteLimitBytes)
	c.sendMu.Unlock()
	for n > 0 {
		if err := c.writeBatch(sendBatchBuffer, n); err != nil {
			return err
		}
		c.sendMu.Lock()
		n = c.sendQueue.PopMany(sbbLen, sendBatchBuffer, c.opts.WpOptions.BatchWriteLimitBytes)
		c.sendMu.Unlock()
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
	if n <= 0 {
		return nil
	}

	if n > 1 {
		bs := make([][]byte, n)
		totalBytes := 0
		for i := 0; i < n; i++ {
			bs[i] = batch[i].B
			totalBytes += len(bs[i])
		}
		if err := c.conn.AsyncWritev(bs, func(_ gnet.Conn, err error) error {
			if err != nil {
				if c.stats != nil {
					c.stats.AddError()
				}
			} else {
				if c.stats != nil {
					c.stats.AddSent(totalBytes)
				}
				for j := 0; j < n; j++ {
					kkbuffer.Put(batch[j])
					batch[j] = nil
				}
			}
			return nil
		}); err != nil {
			if c.stats != nil {
				c.stats.AddError()
			}
			return err
		}
		return nil
	} else {
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
}
