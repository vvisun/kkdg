package kkgws

import (
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/lxzan/gws"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kktime"
	"github.com/vvisun/kkdg/utils/timingwheel"
)

const sessionKeyConn = "_kkgws"
const enableWP = false

type gwsConn struct {
	id     kknet.CONN_ID
	socket *gws.Conn
	opts   *kknet.Options
	stats  *kknet.Stats

	closeOnce sync.Once
	closing   atomic.Bool

	writeMu sync.Mutex
	wp      kknet.IWriteProcessor
	rp      kknet.IReadProcessor

	pingTimer unsafe.Pointer // *timingwheel.Timer

	extraMu   sync.RWMutex
	extraData any // 自定义数据
}

var _ kknet.IConn = (*gwsConn)(nil)

func newGwsConn(socket *gws.Conn, opts *kknet.Options, stats *kknet.Stats) *gwsConn {
	kknet.CheckOptions(opts)
	c := &gwsConn{
		id:     kknet.NextConnID(),
		socket: socket,
		opts:   opts,
		stats:  stats,
	}

	if enableWP {
		if opts.WpProvider != nil {
			c.wp = opts.WpProvider(opts.WpOptions)
		} else {
			c.wp = defaultWpProvider(opts.WpOptions)
		}
		c.wp.Start(c, c.writeBatch, func(_ error) { _ = socket.WriteClose(1011, nil) }, c.stats)
	}

	if opts.RpProvider != nil {
		c.rp = opts.RpProvider(opts.RpOptions)
	} else {
		c.rp = defaultRpProvider(opts.RpOptions)
	}
	c.rp.Start(c)

	return c
}

func (c *gwsConn) ID() kknet.CONN_ID {
	return c.id
}

func (c *gwsConn) RemoteAddr() string {
	if c.socket == nil {
		return ""
	}
	addr := c.socket.RemoteAddr()
	if addr == nil {
		return ""
	}
	return addr.String()
}

func (c *gwsConn) SetExtraData(extraData any) {
	c.extraMu.Lock()
	c.extraData = extraData
	c.extraMu.Unlock()
}

func (c *gwsConn) GetExtraData() any {
	c.extraMu.RLock()
	data := c.extraData
	c.extraMu.RUnlock()
	return data
}

func (c *gwsConn) Close() error {
	if c.closing.Swap(true) {
		return nil
	}
	_ = c.socket.WriteClose(1000, nil)
	return nil
}

// onRecvMessage is called from the gws OnMessage callback.
func (c *gwsConn) onRecvMessage(data []byte) {
	if len(data) == 0 {
		return
	}
	if c.stats != nil {
		c.stats.AddRecv(len(data))
	}
	if err := c.rp.OnRecvBytes(data); err != nil {
		if c.stats != nil {
			c.stats.AddError()
		}
		_ = c.socket.WriteClose(1011, nil)
	}
}

// doClose performs the actual cleanup; called from the gws OnClose callback.
func (c *gwsConn) doClose(handler kknet.IConnLifecycleHandler, err error) {
	c.closeOnce.Do(func() {
		c.closing.Store(true)
		c.stopPingByTimingWheel()
		if c.rp != nil {
			c.rp.Stop()
		}
		if c.wp != nil {
			c.wp.Stop(err)
		}

		if c.stats != nil {
			c.stats.OnClose()
			if err != nil {
				c.opts.Logger.Debugf("kkgws OnClose error: connId=%d, err=%v", c.id, err)
				c.stats.AddError()
			}
		}
		if handler != nil {
			kknet.SafeHandlerCall(c.opts.Logger, c.stats, "kkgws OnClose", func() {
				handler.OnClose(c, err)
			})
		}
	})
}

// --- ping keepalive via timing wheel ---

type gwsPingScheduler struct {
	interval time.Duration
}

func (s *gwsPingScheduler) Next(prev time.Time) time.Time {
	return prev.Add(s.interval)
}

func (c *gwsConn) startPingByTimingWheel() {
	if c.opts.PingInterval <= 0 || c.opts.ReadTimeout <= 0 {
		return
	}

	tw := kktime.GetNetTimingWheel()
	t := tw.ScheduleFunc(&gwsPingScheduler{c.opts.PingInterval}, func() {
		if c.closing.Load() {
			return
		}
		if err := c.socket.WritePing(nil); err != nil {
			c.opts.Logger.Errorf("kkgws write ping error: %v", err)
		}
	})
	if t != nil {
		atomic.StorePointer(&c.pingTimer, unsafe.Pointer(t))
	}
}

func (c *gwsConn) stopPingByTimingWheel() {
	if p := atomic.LoadPointer(&c.pingTimer); p != nil {
		(*timingwheel.Timer)(p).Stop()
		atomic.StorePointer(&c.pingTimer, nil)
	}
}

// --- send ---

func (c *gwsConn) SendMsg(msg any) error {
	if msg == nil {
		return kkerrors.ErrClusterInvalidPacket
	}
	if c.closing.Load() {
		return kkerrors.ErrNetConnectionClosed
	}
	if enableWP {
		if c.wp == nil {
			return kkerrors.ErrNetConnectionClosed
		}
		return c.wp.SendMsg(msg)
	}
	buffer, err := kkpacket.EncodeStream(msg, c.opts.StreamTool, c.opts.WpOptions.MsgPacket)
	if err != nil {
		kkbuffer.Put(buffer)
		return err
	}
	return c.SendBuffer(buffer)
}

func (c *gwsConn) SendBuffer(buffer *kkbuffer.ByteBuffer) error {
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
	if enableWP {
		if c.wp == nil {
			kkbuffer.Put(buffer)
			return kkerrors.ErrNetConnectionClosed
		}
		return c.wp.SendBuffer(buffer)
	}
	c.socket.WriteAsync(gws.OpcodeBinary, buffer.B, func(err error) {
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
	})
	return nil
}

/**writeBatch is the WriteFunc called by the write processor.
 *批量写入。WriteFunc中，发送失败的数据不释放，供调用方知道哪些数据发送失败。
 *@param batch 批量缓冲区，数组长度为 WriteOptions.WriteBatchSize
 *@param n 批量数量
 *@return error
 */
func (c *gwsConn) writeBatch(batch []*kkbuffer.ByteBuffer, n int) error {
	if n <= 0 {
		return nil
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if n > 1 {
		bs := make([][]byte, n)
		totalBytes := 0
		for i := 0; i < n; i++ {
			bs[i] = batch[i].B
			totalBytes += len(bs[i])
		}
		err := c.socket.Writev(gws.OpcodeBinary, bs...)
		if err != nil {
			return err
		}
		for j := 0; j < n; j++ {
			kkbuffer.Put(batch[j])
			batch[j] = nil
		}
		if c.stats != nil {
			c.stats.AddSent(totalBytes)
		}
		return nil
	}

	bb := batch[0]
	err := c.socket.WriteMessage(gws.OpcodeBinary, bb.B)
	if err != nil {
		return err
	}
	if c.stats != nil {
		c.stats.AddSent(len(bb.B))
	}
	kkbuffer.Put(bb)
	batch[0] = nil
	return nil
}

// getGwsConn retrieves the gwsConn stored in the gws.Conn session.
func getGwsConn(socket *gws.Conn) *gwsConn {
	v, ok := socket.Session().Load(sessionKeyConn)
	if !ok {
		return nil
	}
	return v.(*gwsConn)
}
