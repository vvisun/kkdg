package kkws

import (
	"context"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

type wsConn struct {
	id    kknet.CONN_ID
	conn  *websocket.Conn
	opts  kknet.Options
	stats *kknet.Stats

	writeMu   sync.Mutex
	closeOnce sync.Once

	ctxMu sync.RWMutex
	ctx   context.Context
}

var _ kknet.IConn = (*wsConn)(nil)

func newWSConn(conn *websocket.Conn, opts kknet.Options, stats *kknet.Stats) *wsConn {
	return &wsConn{
		id:    kknet.NextConnID(),
		conn:  conn,
		opts:  opts,
		stats: stats,
		ctx:   context.Background(),
	}
}

func (c *wsConn) ID() kknet.CONN_ID {
	return c.id
}

func (c *wsConn) RemoteAddr() string {
	if c.conn == nil || c.conn.UnderlyingConn() == nil {
		return ""
	}
	return c.conn.UnderlyingConn().RemoteAddr().String()
}

func (c *wsConn) SendBuffer(buffer buffers.IBuffer) error {
	if err := kkpacket.DefaultStreamPacket().CheckPacketBuffer(buffer); err != nil {
		if c.stats != nil {
			c.stats.AddError()
		}
		return err
	}

	data := buffer.B

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	// Update write deadline if timeout is configured
	if c.opts.WsWriteTimeout > 0 {
		if err := c.conn.SetWriteDeadline(time.Now().Add(c.opts.WsWriteTimeout)); err != nil {
			if c.stats != nil {
				c.stats.AddError()
			}
			return err
		}
	}

	if err := c.conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
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

func (c *wsConn) Close() error {
	c.closeWithError(nil, nil)
	return nil
}

func (c *wsConn) Context() context.Context {
	c.ctxMu.RLock()
	defer c.ctxMu.RUnlock()
	return c.ctx
}

func (c *wsConn) SetContext(ctx context.Context) {
	c.ctxMu.Lock()
	c.ctx = ctx
	c.ctxMu.Unlock()
}

func (c *wsConn) readLoop(dispatch func(kknet.IConn, buffers.IBuffer)) error {
	for {
		// Update read deadline if timeout is configured
		if c.opts.WsReadTimeout > 0 {
			if err := c.conn.SetReadDeadline(time.Now().Add(c.opts.WsReadTimeout)); err != nil {
				return err
			}
		}

		_, data, err := c.conn.ReadMessage()
		if err != nil {
			return err
		}
		if c.stats != nil {
			c.stats.AddRecv(len(data))
		}

		if dispatch != nil {
			payload := kkbuffer.GetWithCapacity(len(data))
			payload.B = payload.B[:len(data)]
			copy(payload.B, data)
			dispatch(c, payload)
		}
	}
}

func (c *wsConn) closeWithError(handler kknet.IHandler, err error) {
	c.closeOnce.Do(func() {
		if c.stats != nil {
			c.stats.OnClose()
			if err != nil {
				c.stats.AddError()
			}
		}
		_ = c.conn.Close()
		if handler != nil {
			kknet.SafeHandlerCall(c.opts.Logger, c.stats, "kkws OnClose", func() {
				handler.OnClose(c, err)
			})
		}
	})
}
