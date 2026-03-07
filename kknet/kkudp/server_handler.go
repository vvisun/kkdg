package kkudp

import (
	"github.com/panjf2000/gnet/v2"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
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

// OnTraffic 必须在一次调用内排空 inbound 缓冲区（gnet level-triggered：
// 若不读完，不会因剩余数据再次触发 OnTraffic，导致压测下大量包堆积、表现差）。
func (h *udpEventHandler) OnTraffic(c gnet.Conn) (action gnet.Action) {
	for {
		size := c.InboundBuffered()
		if size <= 0 {
			return gnet.None
		}
		if size > kkpacket.MaxPacketSize() {
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
			dataCpy := kkbuffer.GetWithCapacity(len(data))
			dataCpy.B = dataCpy.B[:len(data)]
			copy(dataCpy.B, data)
			h.dispatch(uc, dataCpy)
		}
		uc.deactivate()
	}
}

func (h *udpEventHandler) dispatch(c *udpConn, data *kkbuffer.ByteBuffer) {
	// UDP connection is only valid during OnTraffic callback.
	if h.server.opts.RpOptions.RawHandler != nil {
		kknet.SafeHandlerCall(h.server.opts.Logger, &h.server.stats, "kkudp OnMessage", func() {
			h.server.opts.RpOptions.RawHandler.OnRaw(c.id, data)
		})
	}
}
