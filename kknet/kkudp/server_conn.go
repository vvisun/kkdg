package kkudp

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/panjf2000/gnet/v2"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
)

type udpConn struct {
	id         kknet.CONN_ID
	conn       gnet.Conn
	remoteAddr string
	opts       kknet.Options
	active     atomic.Bool
	stats      *kknet.Stats
	lastSeen   atomic.Int64

	ctxMu sync.RWMutex
	ctx   context.Context
}

func newUDPConn(c gnet.Conn, opts kknet.Options, stats *kknet.Stats, now time.Time) *udpConn {
	conn := &udpConn{
		id:         kknet.NextConnID(),
		conn:       c,
		remoteAddr: c.RemoteAddr().String(),
		opts:       opts,
		stats:      stats,
		ctx:        context.Background(),
	}
	conn.active.Store(true)
	conn.lastSeen.Store(now.UnixNano())
	return conn
}

func (c *udpConn) ID() kknet.CONN_ID {
	return c.id
}

func (c *udpConn) RemoteAddr() string {
	return c.remoteAddr
}

func (c *udpConn) Send(data []byte) error {
	if !c.active.Load() {
		return kkerrors.ErrConnectionClosed
	}
	if len(data) > c.opts.MaxMessageSize {
		if c.stats != nil {
			c.stats.AddError()
		}
		return kkerrors.ErrMaxMessageSize
	}
	if c.conn == nil {
		if c.stats != nil {
			c.stats.AddError()
		}
		return kkerrors.ErrConnectionClosed
	}
	_, err := c.conn.Write(data)
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

func (c *udpConn) Close() error {
	return nil
}

func (c *udpConn) Context() context.Context {
	c.ctxMu.RLock()
	defer c.ctxMu.RUnlock()
	return c.ctx
}

func (c *udpConn) SetContext(ctx context.Context) {
	c.ctxMu.Lock()
	c.ctx = ctx
	c.ctxMu.Unlock()
}

func (c *udpConn) deactivate() {
	c.active.Store(false)
}

func (c *udpConn) activate(conn gnet.Conn, now time.Time) {
	c.conn = conn
	c.remoteAddr = conn.RemoteAddr().String()
	c.active.Store(true)
	c.lastSeen.Store(now.UnixNano())
}

func (c *udpConn) isIdle(now time.Time, timeout time.Duration) bool {
	if c.active.Load() {
		return false
	}
	last := c.lastSeen.Load()
	if last == 0 {
		return false
	}
	return now.Sub(time.Unix(0, last)) >= timeout
}
