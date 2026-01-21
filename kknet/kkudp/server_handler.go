package kkudp

import (
	"github.com/panjf2000/gnet/v2"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

type udpEventHandler struct {
	*gnet.BuiltinEventEngine
	server *Server
}

func (h *udpEventHandler) OnBoot(eng gnet.Engine) (action gnet.Action) {
	h.server.engine = eng
	h.server.opts.Logger.Infof("kkudp server listen on %s", h.server.addr)
	close(h.server.booted)
	return gnet.None
}

func (h *udpEventHandler) OnTraffic(c gnet.Conn) (action gnet.Action) {
	size := c.InboundBuffered()
	if size <= 0 {
		return gnet.None
	}
	if size > h.server.opts.MaxMessageSize {
		h.server.stats.AddError()
		h.server.opts.Logger.Errorf("kkudp message too large: %d", size)
		return gnet.None
	}
	data, err := c.Next(size)
	if err != nil {
		h.server.stats.AddError()
		h.server.opts.Logger.Errorf("kkudp read error: %v", err)
		return gnet.None
	}

	uc := h.server.newConn(c)
	h.server.stats.AddRecv(len(data))
	if h.server.handler != nil {
		payload := kkbuffer.Get()
		payload.SetBytes(data)
		h.dispatch(uc, payload)
	}
	uc.deactivate()
	return gnet.None
}

func (h *udpEventHandler) dispatch(c *udpConn, data buffers.IBuffer) {
	// UDP connection is only valid during OnTraffic callback.
	defer kkbuffer.Put(data)
	kknet.SafeHandlerCall(h.server.opts.Logger, &h.server.stats, "kkudp OnMessage", func() {
		h.server.handler.OnMessage(c, data)
	})
}
