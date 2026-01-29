package kktcp

import (
	"github.com/panjf2000/gnet/v2"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

type tcpEventHandler struct {
	*gnet.BuiltinEventEngine
	server *Server
}

func (h *tcpEventHandler) OnBoot(eng gnet.Engine) (action gnet.Action) {
	h.server.engine = eng
	h.server.opts.Logger.Infof("kktcp server listen on %s", h.server.addr)
	close(h.server.booted)
	return gnet.None
}

func (h *tcpEventHandler) OnShutdown(eng gnet.Engine) {
	_ = eng
}

func (h *tcpEventHandler) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	h.server.stats.OnConnect()
	tconn := newTCPConn(c, h.server.opts, &h.server.stats)
	h.server.connMgr.addConn(tconn)
	c.SetContext(tconn)
	if h.server.handler != nil {
		kknet.SafeHandlerCall(h.server.opts.Logger, &h.server.stats, "kktcp OnConnect", func() {
			h.server.handler.OnConnect(tconn)
		})
	}
	return nil, gnet.None
}

func (h *tcpEventHandler) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	h.server.stats.OnClose()
	if err != nil {
		h.server.stats.AddError()
	}
	if tc, ok := c.Context().(*tcpConn); ok {
		h.server.connMgr.removeConn(tc.id)
	}
	if h.server.handler == nil {
		return gnet.None
	}
	if tc, ok := c.Context().(*tcpConn); ok {
		kknet.SafeHandlerCall(h.server.opts.Logger, &h.server.stats, "kktcp OnClose", func() {
			h.server.handler.OnClose(tc, err)
		})
	}
	return gnet.None
}

func (h *tcpEventHandler) OnTraffic(c gnet.Conn) (action gnet.Action) {
	tc, ok := c.Context().(*tcpConn)
	if !ok {
		return gnet.Close
	}

	for {
		data, ok, err := kkpacket.DefaultStreamPacket().UnpackFromSR(c)
		if err != nil {
			h.server.stats.AddError()
			return gnet.Close
		}
		if !ok {
			return gnet.None
		}
		h.server.stats.AddRecv(len(data))
		if h.server.handler != nil {
			payload := kkbuffer.GetWithCapacity(len(data))
			payload.B = payload.B[:len(data)]
			copy(payload.B, data)
			h.dispatch(tc, payload)
		}
	}
}

func (h *tcpEventHandler) dispatch(c *tcpConn, data buffers.IBuffer) {
	if h.server.pool == nil {
		defer kkbuffer.Put(data)
		kknet.SafeHandlerCall(h.server.opts.Logger, &h.server.stats, "kktcp OnMessage", func() {
			h.server.handler.OnMessage(c, data)
		})
		return
	}
	if err := h.server.pool.Submit(func() {
		defer kkbuffer.Put(data)
		kknet.SafeHandlerCall(h.server.opts.Logger, &h.server.stats, "kktcp OnMessage", func() {
			h.server.handler.OnMessage(c, data)
		})
	}); err != nil {
		kkbuffer.Put(data)
		h.server.opts.Logger.Errorf("kktcp submit task error: %v", err)
	}
}
