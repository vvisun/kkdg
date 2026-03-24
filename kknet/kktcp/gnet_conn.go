package kktcp

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/panjf2000/gnet/v2"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/internal"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/queues/bbqueue"
)

type gnetConn struct {
	id    kknet.CONN_ID
	conn  gnet.Conn
	opts  *kknet.Options
	stats *kknet.Stats

	closing atomic.Bool

	rp kknet.IReadProcessor

	stopReadOnce sync.Once
	readStopped  chan struct{}

	closeMu    sync.Mutex
	closeCond  *sync.Cond
	sendQueue  *bbqueue.BBQueue
	draining   bool
	drainBatch [kknet.BatchPacketSize]*kkbuffer.ByteBuffer
}

var _ kknet.IConn = (*gnetConn)(nil)

func newGnetConn(c gnet.Conn, opts *kknet.Options, stats *kknet.Stats) *gnetConn {
	kknet.CheckOptions(opts)
	gc := &gnetConn{
		id:          internal.NextConnID(),
		conn:        c,
		opts:        opts,
		stats:       stats,
		readStopped: make(chan struct{}),
		sendQueue: bbqueue.NewBBQueue(
			opts.WpOptions.SendQueueSize,
			opts.WpOptions.SendQueueStrict,
		),
	}
	gc.closeCond = sync.NewCond(&gc.closeMu)
	if opts.RpProvider != nil {
		gc.rp = opts.RpProvider(opts.RpOptions)
	} else {
		gc.rp = defaultRpProvider(opts.RpOptions)
	}
	gc.rp.Start(gc)

	return gc
}

func (c *gnetConn) ID() kknet.CONN_ID {
	return c.id
}

func (c *gnetConn) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}

func (c *gnetConn) Close() error {
	if c.closing.Swap(true) {
		return nil
	}
	timedOut := false
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
				timedOut = true
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
	}
	if timedOut || !c.opts.WpOptions.SendQueueNeedFlushOver {
		c.dropQueuedBuffers()
	}
	err := c.conn.Close()
	c.stopReadAndWait()
	return err
}

func (c *gnetConn) SendMsg(msg any) error {
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

func (c *gnetConn) SendBuffer(buffer *kkbuffer.ByteBuffer) error {
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

func (c *gnetConn) sendBufferDrop(buffer *kkbuffer.ByteBuffer) error {
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

func (c *gnetConn) sendBufferRetry(buffer *kkbuffer.ByteBuffer) error {
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

func (c *gnetConn) startDrain() {
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

	if err := c.conn.AsyncWritev(payloads, func(_ gnet.Conn, err error) error {
		c.releaseDrainedBatch(n)
		if err != nil {
			if c.stats != nil {
				c.stats.AddError()
			}
			c.handleWriteError(err)
			return nil
		}
		if c.stats != nil {
			c.stats.AddSent(totalBytes)
		}
		c.closeMu.Lock()
		if c.sendQueue.Len() > 0 {
			c.closeMu.Unlock()
			c.startDrain()
			return nil
		}
		c.draining = false
		c.closeCond.Broadcast()
		c.closeMu.Unlock()
		return nil
	}); err != nil {
		c.releaseDrainedBatch(n)
		if c.stats != nil {
			c.stats.AddError()
		}
		c.handleWriteError(err)
	}
}

func (c *gnetConn) releaseDrainedBatch(n int) {
	for i := 0; i < n; i++ {
		bb := c.drainBatch[i]
		c.drainBatch[i] = nil
		if bb != nil {
			kkbuffer.Put(bb)
		}
	}
}

func (c *gnetConn) handleWriteError(err error) {
	c.closing.Store(true)
	c.dropQueuedBuffers()
	_ = c.conn.Close()
	_ = err
}

func (c *gnetConn) handleUnderlyingClose() {
	c.closing.Store(true)
	c.dropQueuedBuffers()
	c.stopReadAsync()
}

func (c *gnetConn) dropQueuedBuffers() {
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

func (c *gnetConn) stopReadAsync() {
	c.stopReadOnce.Do(func() {
		go c.stopRead()
	})
}

func (c *gnetConn) stopReadAndWait() {
	c.stopReadOnce.Do(func() {
		c.stopRead()
	})
	<-c.readStopped
}

func (c *gnetConn) stopRead() {
	if c.rp != nil {
		c.rp.Stop()
	}
	close(c.readStopped)
}
