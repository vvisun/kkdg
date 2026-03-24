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
	tconn := newGnetConn(c, &h.server.opts, &h.server.stats)
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
	var tc *gnetConn
	if conn, ok := c.Context().(*gnetConn); ok {
		tc = conn
		tc.handleUnderlyingClose()
		h.server.connMgr.RemoveConn(tc.id)
	}
	if tc != nil && h.server.handler != nil {
		go func() {
			tc.stopReadAndWait()
			kknet.SafeHandlerCall(h.server.opts.Logger, &h.server.stats, "kktcp OnClose", func() {
				h.server.handler.OnClose(tc, err)
			})
		}()
		return gnet.None
	}
	return gnet.None
}

func (h *tcpEventHandler) OnTraffic(c gnet.Conn) (action gnet.Action) {
	tc, ok := c.Context().(*gnetConn)
	if !ok {
		return gnet.Close
	}
	if tc.closing.Load() {
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
		if tc.closing.Load() {
			return gnet.Close
		}
		if tc.rp != nil {
			if err := tc.rp.EnqueuePacket(data); err != nil {
				h.server.stats.AddError()
				return gnet.Close
			}
		}
	}
}
