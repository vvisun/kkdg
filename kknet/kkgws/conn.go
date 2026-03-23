package kkgws

import (
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/lxzan/gws"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/internal"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kktime"
	"github.com/vvisun/kkdg/utils/queues/bbqueue"
	"github.com/vvisun/kkdg/utils/timingwheel"
)

const sessionKeyConn = "_kkgws"

type gwsConn struct {
	id      kknet.CONN_ID
	socket  *gws.Conn
	opts    *kknet.Options
	stats   *kknet.Stats
	handler kknet.IConnLifecycleHandler

	closeOnce sync.Once
	closing   atomic.Bool

	rp kknet.IReadProcessor

	closeMu    sync.Mutex
	closeCond  *sync.Cond
	sendQueue  *bbqueue.BBQueue
	draining   bool
	drainBatch [kknet.BatchPacketSize]*kkbuffer.ByteBuffer

	pingTimer unsafe.Pointer // *timingwheel.Timer
}

var _ kknet.IConn = (*gwsConn)(nil)

func newGwsConn(socket *gws.Conn, opts *kknet.Options, stats *kknet.Stats, handler kknet.IConnLifecycleHandler) *gwsConn {
	kknet.CheckOptions(opts)
	c := &gwsConn{
		id:      internal.NextConnID(),
		socket:  socket,
		opts:    opts,
		stats:   stats,
		handler: handler,
		sendQueue: bbqueue.NewBBQueue(
			opts.WpOptions.SendQueueSize,
			opts.WpOptions.SendQueueStrict,
		),
	}
	c.closeCond = sync.NewCond(&c.closeMu)

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

func (c *gwsConn) Close() error {
	if c.closing.Swap(true) {
		return nil
	}
	if c.opts.WpOptions.SendQueueNeedFlushOver {
		timeout := c.opts.WpOptions.SendQueueTimeoutFlushOver
		if timeout <= 0 {
			timeout = 10 * time.Second
		}
		deadline := time.Now().Add(timeout)
		c.closeMu.Lock()
		for {
			if c.sendQueue.Len() == 0 && !c.draining {
				c.closeMu.Unlock()
				break
			}
			remain := time.Until(deadline)
			if remain <= 0 {
				c.closeMu.Unlock()
				if c.opts.WpOptions.SendQueueFlushTimeoutCallback != nil {
					c.opts.WpOptions.SendQueueFlushTimeoutCallback(c, timeout)
				}
				break
			}
			timer := time.AfterFunc(remain, func() {
				c.closeMu.Lock()
				c.closeCond.Broadcast()
				c.closeMu.Unlock()
			})
			c.closeCond.Wait()
			_ = timer.Stop()
		}
	} else {
		c.dropQueuedBuffers()
	}

	if c.socket != nil {
		_ = c.socket.WriteClose(1000, nil)
	}

	c.doClose(c.handler, nil)
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
		if c.socket != nil {
			_ = c.socket.WriteClose(1011, nil)
		}
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
		if c.closing.Load() || c.socket == nil {
			return
		}
		if err := c.socket.WritePing(nil); err != nil {
			c.opts.Logger.Errorf("kkgws write ping error, closing connection: %v", err)
			_ = c.socket.WriteClose(1011, nil)
			return
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
	switch c.opts.WpOptions.SendQueueFullAction {
	case kknet.EWpQueueFullActionRetry:
		return c.sendBufferRetry(buffer)
	case kknet.EWpQueueFullActionDrop:
		return c.sendBufferDrop(buffer)
	default:
		return c.sendBufferDrop(buffer)
	}
}

func (c *gwsConn) sendBufferDrop(buffer *kkbuffer.ByteBuffer) error {
	c.closeMu.Lock()
	if c.closing.Load() {
		c.closeMu.Unlock()
		kkbuffer.Put(buffer)
		return kkerrors.ErrNetConnectionClosed
	}
	ok := c.sendQueue.Push(buffer)
	if !ok {
		c.closeMu.Unlock()
		kkbuffer.Put(buffer)
		return nil
	}
	shouldDrain := !c.draining
	if shouldDrain {
		c.draining = true
	}
	c.closeMu.Unlock()
	if shouldDrain {
		c.startDrain()
	}
	return nil
}

func (c *gwsConn) sendBufferRetry(buffer *kkbuffer.ByteBuffer) error {
	interval := c.opts.WpOptions.SendQueueRetryInterval
	if interval <= 0 {
		interval = 2 * time.Millisecond
	}
	maxCount := c.opts.WpOptions.SendQueueRetryMaxCount

	for i := 0; ; i++ {
		c.closeMu.Lock()
		if c.closing.Load() {
			c.closeMu.Unlock()
			kkbuffer.Put(buffer)
			return kkerrors.ErrNetConnectionClosed
		}
		ok := c.sendQueue.Push(buffer)
		if ok {
			shouldDrain := !c.draining
			if shouldDrain {
				c.draining = true
			}
			c.closeMu.Unlock()
			if shouldDrain {
				c.startDrain()
			}
			return nil
		}
		c.closeMu.Unlock()
		if maxCount > 0 && i >= maxCount-1 {
			kkbuffer.Put(buffer)
			return kkerrors.ErrNetSendQueueFull
		}
		time.Sleep(interval)
	}
}

func (c *gwsConn) startDrain() {
	c.closeMu.Lock()
	n := c.sendQueue.PopMany(len(c.drainBatch), c.drainBatch[:], c.opts.WpOptions.BatchWriteLimitBytes)
	if n <= 0 {
		c.draining = false
		c.closeCond.Broadcast()
		c.closeMu.Unlock()
		return
	}
	payloads := make([][]byte, 0, n)
	totalBytes := 0
	for i := 0; i < n; i++ {
		bb := c.drainBatch[i]
		if bb == nil {
			continue
		}
		payloads = append(payloads, bb.B)
		totalBytes += len(bb.B)
	}
	c.closeMu.Unlock()

	c.socket.WritevAsync(gws.OpcodeBinary, payloads, func(err error) {
		for i := 0; i < n; i++ {
			bb := c.drainBatch[i]
			c.drainBatch[i] = nil
			if bb != nil {
				kkbuffer.Put(bb)
			}
		}
		if err != nil {
			if c.stats != nil {
				c.stats.AddError()
			}
			c.handleWriteError(err)
			return
		} else if c.stats != nil {
			c.stats.AddSent(totalBytes)
		}
		c.closeMu.Lock()
		if c.sendQueue.Len() > 0 {
			c.closeMu.Unlock()
			c.startDrain()
			return
		}
		c.draining = false
		c.closeCond.Broadcast()
		c.closeMu.Unlock()
	})
}

func (c *gwsConn) handleWriteError(err error) {
	c.closing.Store(true)
	c.dropQueuedBuffers()
	if c.socket != nil {
		_ = c.socket.WriteClose(1011, nil)
	}
	c.doClose(c.handler, err)
}

func (c *gwsConn) dropQueuedBuffers() {
	c.closeMu.Lock()
	c.draining = false
	for {
		bb := c.sendQueue.Pop()
		if bb == nil {
			break
		}
		kkbuffer.Put(bb)
	}
	c.closeCond.Broadcast()
	c.closeMu.Unlock()
}

// getGwsConn retrieves the gwsConn stored in the gws.Conn session.
func getGwsConn(socket *gws.Conn) *gwsConn {
	v, ok := socket.Session().Load(sessionKeyConn)
	if !ok {
		return nil
	}
	return v.(*gwsConn)
}
