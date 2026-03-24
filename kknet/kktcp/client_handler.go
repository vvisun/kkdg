package kktcp

import (
	"github.com/panjf2000/gnet/v2"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
)

type gnetClientEventHandler struct {
	*gnet.BuiltinEventEngine
	client *GnetClient
}

func (h *gnetClientEventHandler) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	h.client.stats.OnConnect()
	if kknet.LoadConnStatus(&h.client.status) == kknet.ConnStatusReconnecting {
		kknet.ChangeConnStatus(&h.client.status, kknet.ConnStatusReconnected)
	} else {
		kknet.ChangeConnStatus(&h.client.status, kknet.ConnStatusConnected)
	}
	cc := newGnetConn(c, &h.client.opts, &h.client.stats)
	c.SetContext(cc)
	h.client.opts.Logger.Infof("kktcp client connect success... connId=%d", cc.id)

	h.client.connMu.Lock()
	h.client.conn = cc
	h.client.connMu.Unlock()
	h.client.signalOpenResult(nil)

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
	var cc *gnetConn
	if conn, ok := c.Context().(*gnetConn); ok {
		cc = conn
		cc.handleUnderlyingClose()
	}
	h.client.connMu.Lock()
	h.client.conn = nil
	h.client.connMu.Unlock()
	if cc != nil {
		go func() {
			cc.stopReadAndWait()
			if h.client.handler != nil {
				kknet.SafeHandlerCall(h.client.opts.Logger, &h.client.stats, "gnetclient OnClose", func() {
					h.client.handler.OnClose(cc, err)
				})
			}
			if !kknet.IsClosingOrClosed(&h.client.status) && h.client.opts.IsNeedReconnect {
				h.client.startReconnect()
			}
		}()
		return gnet.None
	}
	h.client.signalOpenResult(normalizeOpenWaitError(err))
	if !kknet.IsClosingOrClosed(&h.client.status) && h.client.opts.IsNeedReconnect {
		h.client.startReconnect()
	}
	return gnet.None
}

func normalizeOpenWaitError(err error) error {
	if err != nil {
		return err
	}
	return kkerrors.ErrNetConnectionClosed
}

func (h *gnetClientEventHandler) OnTraffic(c gnet.Conn) (action gnet.Action) {
	cc, ok := c.Context().(*gnetConn)
	if !ok {
		return gnet.Close
	}
	if cc.closing.Load() {
		return gnet.Close
	}
	streamTool := cc.opts.StreamTool
	for {
		data, ok, err := streamTool.SplitSR(c)
		if err != nil {
			h.client.stats.AddError()
			return gnet.Close
		}
		if !ok {
			return gnet.None
		}
		h.client.stats.AddRecv(len(data))
		if cc.closing.Load() {
			return gnet.Close
		}
		if cc.rp != nil {
			if err := cc.rp.EnqueuePacket(data); err != nil {
				h.client.stats.AddError()
				return gnet.Close
			}
		}
	}
}
