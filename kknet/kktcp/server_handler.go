package kktcp

import (
	"github.com/panjf2000/gnet/v2"
	"github.com/vvisun/kkdg/kknet"
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
	tconn := newTCPConn(c, &h.server.opts, &h.server.stats)
	h.server.connMgr.AddConn(tconn)
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
		// Stop read processor asynchronously (drain remaining queue outside event-loop).
		if tc.rp != nil {
			go tc.rp.Stop()
		}
		h.server.connMgr.RemoveConn(tc.id)
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
	streamTool := tc.opts.StreamTool
	for {
		data, ok, err := streamTool.SplitSR(c)
		if err != nil {
			h.server.stats.AddError()
			return gnet.Close
		}
		if !ok {
			return gnet.None
		}
		h.server.stats.AddRecv(len(data))
		// feed into ReadProcessor; it will copy into pooled buffers and dispatch asynchronously.
		if tc.rp != nil {
			tc.rp.EnqueuePacket(data)
		}
	}
}
