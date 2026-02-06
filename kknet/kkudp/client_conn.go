package kkudp

import (
	"context"
	"net"
	"sync"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

type clientConn struct {
	id    kknet.CONN_ID
	conn  *net.UDPConn
	opts  kknet.Options
	stats *kknet.Stats

	writeMu   sync.Mutex
	closeOnce sync.Once

	ctxMu sync.RWMutex
	ctx   context.Context
}

var _ kknet.IConn = (*clientConn)(nil)

func newClientConn(conn *net.UDPConn, opts kknet.Options, stats *kknet.Stats) *clientConn {
	return &clientConn{
		id:    kknet.NextConnID(),
		conn:  conn,
		opts:  opts,
		stats: stats,
		ctx:   context.Background(),
	}
}

func (c *clientConn) ID() kknet.CONN_ID {
	return c.id
}

func (c *clientConn) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}

func (c *clientConn) SendMsg(msg any) error {
	if msg == nil {
		return kkerrors.ErrInvalidPacket
	}
	buffer, err := kkpacket.DefaultStreamPacket().Encode(msg, nil)
	if err != nil {
		return err
	}
	return c.SendBuffer(buffer)
}

func (c *clientConn) SendBuffer(buffer *kkbuffer.ByteBuffer) error {
	if err := kkpacket.DefaultStreamPacket().CheckPacketBuffer(buffer); err != nil {
		if c.stats != nil {
			c.stats.AddError()
		}
		kkbuffer.Put(buffer)
		return err
	}
	data := buffer.B
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_, err := c.conn.Write(data)
	kkbuffer.Put(buffer)
	if err != nil {
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

func (c *clientConn) Close() error {
	return c.conn.Close()
}

func (c *clientConn) Context() context.Context {
	c.ctxMu.RLock()
	defer c.ctxMu.RUnlock()
	return c.ctx
}

func (c *clientConn) SetContext(ctx context.Context) {
	c.ctxMu.Lock()
	c.ctx = ctx
	c.ctxMu.Unlock()
}

func (c *clientConn) readLoop() error {
	buf := make([]byte, kkpacket.DefaultMaxMessageSize())
	for {
		n, err := c.conn.Read(buf)
		if err != nil {
			return err
		}
		if c.stats != nil {
			c.stats.AddRecv(n)
		}
		if c.opts.RpOptions.RawHandler != nil && n > 0 {
			dataCpy := kkbuffer.GetWithCapacity(n)
			dataCpy.B = dataCpy.B[:n]
			copy(dataCpy.B, buf[:n])
			kknet.SafeHandlerCall(c.opts.Logger, c.stats, "udpclient OnMessage", func() {
				c.opts.RpOptions.RawHandler.OnRaw(c.id, dataCpy)
			})
		}
	}
}

func (c *clientConn) closeWithError(handler kknet.IConnLifecycleHandler, err error) {
	c.closeOnce.Do(func() {
		if c.stats != nil {
			c.stats.OnClose()
			if err != nil {
				c.stats.AddError()
			}
		}
		_ = c.conn.Close()
		if handler != nil {
			kknet.SafeHandlerCall(c.opts.Logger, c.stats, "udpclient OnClose", func() {
				handler.OnClose(c, err)
			})
		}
	})
}
