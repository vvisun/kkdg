package kktcp

import (
	"github.com/panjf2000/gnet/v2"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
)

type gnetClientEventHandler struct {
	*gnet.BuiltinEventEngine
	client *GnetClient
}

func (h *gnetClientEventHandler) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	h.client.stats.OnConnect()
	h.client.connected.Store(true)
	h.client.reconnecting.Store(false)
	cc := newGnetClientConn(c, &h.client.opts, &h.client.stats)
	c.SetContext(cc)
	h.client.opts.Logger.Infof("kktcp client connect success... connId=%d", cc.id)

	h.client.connMu.Lock()
	h.client.conn = cc
	openCh := h.client.openCh
	h.client.openCh = nil
	h.client.connMu.Unlock()
	if openCh != nil {
		close(openCh)
	}

	if h.client.handler != nil {
		kknet.SafeHandlerCall(h.client.opts.Logger, &h.client.stats, "gnetclient OnConnect", func() {
			h.client.handler.OnConnect(cc)
		})
	}
	return nil, gnet.None
}

func (h *gnetClientEventHandler) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	h.client.stats.OnClose()
	if err != nil {
		h.client.stats.AddError()
	}
	if cc, ok := c.Context().(*gnetClientConn); ok && h.client.handler != nil {
		// Stop read processor asynchronously (drain remaining queue outside event-loop).
		if cc.rp != nil {
			go cc.rp.Stop()
		}
		// Stop write processor asynchronously (release pending buffers).
		if cc.wp != nil {
			go cc.wp.Stop(err)
		}
		kknet.SafeHandlerCall(h.client.opts.Logger, &h.client.stats, "gnetclient OnClose", func() {
			h.client.handler.OnClose(cc, err)
		})
	}
	h.client.connMu.Lock()
	h.client.conn = nil
	h.client.connMu.Unlock()
	h.client.connected.Store(false)
	if !h.client.closing.Load() && h.client.opts.IsNeedReconnect {
		h.client.startReconnect()
	}
	return gnet.None
}

func (h *gnetClientEventHandler) OnTraffic(c gnet.Conn) (action gnet.Action) {
	cc, ok := c.Context().(*gnetClientConn)
	if !ok {
		return gnet.Close
	}
	for {
		data, ok, err := kkpacket.DefaultStreamPacket().SplitSR(c)
		if err != nil {
			h.client.stats.AddError()
			return gnet.Close
		}
		if !ok {
			return gnet.None
		}
		h.client.stats.AddRecv(len(data))
		if cc.rp != nil {
			cc.rp.EnqueuePacket(data)
		}
	}
}
