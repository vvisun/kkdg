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

	closing atomic.Bool

	rp kknet.IReadProcessor
	wp kknet.IWriteProcessor

	extraData any // 自定义数据
	extraMu   sync.RWMutex
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
		tc.wp = defaultWpProvider(opts.WpOptions)
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

func (c *tcpConn) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}

func (c *tcpConn) SetExtraData(extraData any) {
	c.extraMu.Lock()
	c.extraData = extraData
	c.extraMu.Unlock()
}

func (c *tcpConn) GetExtraData() any {
	c.extraMu.RLock()
	data := c.extraData
	c.extraMu.RUnlock()
	return data
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

func (c *tcpConn) SendBuffer(buffer *kkbuffer.ByteBuffer) error {
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
	if c.wp == nil {
		kkbuffer.Put(buffer)
		return kkerrors.ErrNetConnectionClosed
	}
	return c.wp.SendBuffer(buffer)
}

/*
*批量写入。WriteFunc中，发送失败的数据不释放，供调用方知道哪些数据发送失败。
 * 使用 EventLoop.Execute 将写操作调度到 gnet 事件循环内执行，避免 AsyncWrite 回调中阻塞。
 *@param batch 批量缓冲区，数组长度为 WriteOptions.WriteBatchSize
 *@param n 批量数量
 *@return error
*/
func (c *tcpConn) writeBatch(batch []*kkbuffer.ByteBuffer, n int) error {
	if n <= 0 {
		return nil
	}
	if c.conn == nil {
		return kkerrors.ErrNetConnectionClosed
	}

	resultCh := make(chan error, 1)
	el := c.conn.EventLoop()

	if n > 1 {
		bs := make([][]byte, n)
		totalBytes := 0
		for i := 0; i < n; i++ {
			bs[i] = batch[i].B
			totalBytes += len(bs[i])
		}
		bat := batch
		total := totalBytes
		if err := el.Execute(context.Background(), gnet.RunnableFunc(func(ctx context.Context) error {
			_, writeErr := c.conn.Writev(bs)
			if writeErr != nil {
				if c.stats != nil {
					c.stats.AddError()
				}
				resultCh <- writeErr
				return nil
			}
			if c.stats != nil {
				c.stats.AddSent(total)
			}
			for j := 0; j < len(bat); j++ {
				kkbuffer.Put(bat[j])
				bat[j] = nil
			}
			resultCh <- nil
			return nil
		})); err != nil {
			if c.stats != nil {
				c.stats.AddError()
			}
			for j := 0; j < n; j++ {
				kkbuffer.Put(batch[j])
			}
			return err
		}
		return <-resultCh
	}

	bb := batch[0]
	if bb == nil {
		return nil
	}
	data := bb.B
	if err := el.Execute(context.Background(), gnet.RunnableFunc(func(ctx context.Context) error {
		_, writeErr := c.conn.Write(data)
		if writeErr != nil {
			if c.stats != nil {
				c.stats.AddError()
			}
			resultCh <- writeErr
			return nil
		}
		if c.stats != nil {
			c.stats.AddSent(len(data))
		}
		kkbuffer.Put(bb)
		batch[0] = nil
		resultCh <- nil
		return nil
	})); err != nil {
		if c.stats != nil {
			c.stats.AddError()
		}
		kkbuffer.Put(bb)
		return err
	}
	return <-resultCh
}
