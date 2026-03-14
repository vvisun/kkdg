package kktcptls

import (
	"crypto/tls"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/byteslice"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

type tlsConn struct {
	id    kknet.CONN_ID
	conn  net.Conn
	opts  *kknet.Options
	stats *kknet.Stats

	closing   atomic.Bool
	closeOnce sync.Once

	wp kknet.IWriteProcessor

	extraData any // 自定义数据
	extraMu   sync.RWMutex
}

var _ kknet.IConn = (*tlsConn)(nil)

func newTLSConn(conn net.Conn, opts *kknet.Options, stats *kknet.Stats) *tlsConn {
	kknet.CheckOptions(opts)
	c := &tlsConn{
		id:    kknet.NextConnID(),
		conn:  conn,
		opts:  opts,
		stats: stats,
	}

	if opts.WpProvider != nil {
		c.wp = opts.WpProvider(opts.WpOptions)
	} else {
		c.wp = defaultWpProvider(opts.WpOptions)
	}
	c.wp.Start(c, c.writeBatch, func(_ error) { _ = c.conn.Close() }, c.stats)

	return c
}

func (c *tlsConn) ID() kknet.CONN_ID { return c.id }

func (c *tlsConn) RemoteAddr() string {
	if c.conn == nil {
		return ""
	}
	return c.conn.RemoteAddr().String()
}

func (c *tlsConn) SetExtraData(extraData any) {
	c.extraMu.Lock()
	c.extraData = extraData
	c.extraMu.Unlock()
}

func (c *tlsConn) GetExtraData() any {
	c.extraMu.RLock()
	data := c.extraData
	c.extraMu.RUnlock()
	return data
}

func (c *tlsConn) Close() error {
	c.closeWithError(nil, nil)
	return nil
}

func (c *tlsConn) closeWithError(handler kknet.IConnLifecycleHandler, err error) {
	c.closeOnce.Do(func() {
		c.closing.Store(true)
		if c.wp != nil {
			c.wp.Stop(err)
		}
		if c.stats != nil {
			c.stats.OnClose()
			if err != nil {
				c.opts.Logger.Debugf("kktcptls OnClose error: connId=%d, err=%v", c.id, err)
				c.stats.AddError()
			}
		}
		_ = c.conn.Close()
		if handler != nil {
			kknet.SafeHandlerCall(c.opts.Logger, c.stats, "kktcptls OnClose", func() {
				handler.OnClose(c, err)
			})
		}
	})
}

func (c *tlsConn) readLoop() error {
	var rp kknet.IReadProcessor
	if c.opts.RpProvider != nil {
		rp = c.opts.RpProvider(c.opts.RpOptions)
	} else {
		rp = defaultRpProvider(c.opts.RpOptions)
	}
	rp.Start(c)
	defer rp.Stop()

	buf := make([]byte, c.opts.ReadBufferSize)
	for {
		if c.opts.ReadTimeout > 0 {
			if err := c.conn.SetReadDeadline(time.Now().Add(c.opts.ReadTimeout)); err != nil {
				return err
			}
		}

		n, err := c.conn.Read(buf)
		if n > 0 {
			if c.stats != nil {
				c.stats.AddRecv(n)
			}
			if rpErr := rp.OnRecvBytes(buf[:n]); rpErr != nil {
				if c.stats != nil {
					c.stats.AddError()
				}
				rp.Stop()
				return rpErr
			}
		}
		if err != nil {
			rp.Stop()
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

// -------------------- send --------------------

func (c *tlsConn) SendMsg(msg any) error {
	if msg == nil {
		return kkerrors.ErrClusterInvalidPacket
	}
	if c.closing.Load() {
		return kkerrors.ErrNetConnectionClosed
	}
	if c.wp == nil {
		return kkerrors.ErrNetConnectionClosed
	}
	return c.wp.SendMsg(msg)
}

func (c *tlsConn) SendBuffer(buffer *kkbuffer.ByteBuffer) error {
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

// writeBatch is called by the WriteProcessor goroutine (serial, no concurrent calls).
func (c *tlsConn) writeBatch(batch []*kkbuffer.ByteBuffer, n int) error {
	if n <= 0 {
		return nil
	}

	if c.opts.WriteTimeout > 0 {
		if err := c.conn.SetWriteDeadline(time.Now().Add(c.opts.WriteTimeout)); err != nil {
			return err
		}
	}

	if n > 1 {
		totalBytes := 0
		for i := 0; i < n; i++ {
			bb := batch[i]
			if bb == nil {
				continue
			}
			totalBytes += len(bb.B)
		}
		batchBytes := byteslice.GetZero(totalBytes)
		defer byteslice.Put(batchBytes)

		start := 0
		for i := 0; i < n; i++ {
			bb := batch[i]
			if bb == nil {
				continue
			}
			batchBytes = append(batchBytes, bb.B...)
			if len(batchBytes) >= c.opts.WpOptions.BatchWriteLimitBytes {
				if err := c.writeAll(batchBytes); err != nil {
					return err
				}
				batchBytes = batchBytes[:0]
				for j := start; j <= i; j++ {
					bb2 := batch[j]
					batch[j] = nil
					if bb2 != nil {
						kkbuffer.Put(bb2)
					}
				}
				start = i + 1
			}
		}

		if len(batchBytes) > 0 {
			if err := c.writeAll(batchBytes); err != nil {
				return err
			}
		}

		for j := start; j < n; j++ {
			bb := batch[j]
			batch[j] = nil
			if bb != nil {
				kkbuffer.Put(bb)
			}
		}
		return nil
	}

	bb := batch[0]
	if bb == nil {
		return nil
	}
	if err := c.writeAll(bb.B); err != nil {
		return err
	}
	kkbuffer.Put(bb)
	batch[0] = nil
	return nil
}

func (c *tlsConn) writeAll(data []byte) error {
	for len(data) > 0 {
		n, err := c.conn.Write(data)
		if n > 0 {
			if c.stats != nil {
				c.stats.AddSent(n)
			}
			data = data[n:]
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// -------------------- helpers --------------------

// setTCPKeepAlive enables TCP keepalive on the underlying TCP connection.
func setTCPKeepAlive(conn net.Conn) {
	var tcpConn *net.TCPConn
	switch c := conn.(type) {
	case *net.TCPConn:
		tcpConn = c
	case *tls.Conn:
		if tc, ok := c.NetConn().(*net.TCPConn); ok {
			tcpConn = tc
		}
	}
	if tcpConn != nil {
		_ = tcpConn.SetKeepAlive(true)
		_ = tcpConn.SetKeepAlivePeriod(10 * time.Second)
	}
}
