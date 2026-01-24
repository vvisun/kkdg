package kkwsgob

import (
	"bytes"
	"errors"
	"net/http"
	"net/url"

	"github.com/gobwas/ws"
	"github.com/panjf2000/gnet/v2"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

const (
	maxHandshakeSize = 64 * 1024
)

type wsEventHandler struct {
	*gnet.BuiltinEventEngine
	server *Server
}

func (h *wsEventHandler) OnBoot(eng gnet.Engine) (action gnet.Action) {
	h.server.engine = eng
	h.server.opts.Logger.Infof("kkwsgob server listen on %s%s", h.server.addr, h.server.path)
	close(h.server.booted)
	return gnet.None
}

func (h *wsEventHandler) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	wc := newWSConn(c, h.server.opts, &h.server.stats, ws.StateServerSide)
	c.SetContext(wc)
	return nil, gnet.None
}

func (h *wsEventHandler) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	wc, ok := c.Context().(*wsConn)
	if !ok {
		return gnet.None
	}
	if wc.fragBuf != nil {
		kkbuffer.Put(wc.fragBuf)
		wc.fragBuf = nil
	}
	if !wc.upgraded {
		return gnet.None
	}
	h.server.stats.OnClose()
	if err == nil {
		err = wc.getCloseErr()
	}
	if err != nil {
		h.server.stats.AddError()
	}
	h.server.connMgr.removeConn(wc.id)
	if h.server.handler != nil {
		kknet.SafeHandlerCall(h.server.opts.Logger, &h.server.stats, "kkwsgob OnClose", func() {
			h.server.handler.OnClose(wc, err)
		})
	}
	return gnet.None
}

func (h *wsEventHandler) OnTraffic(c gnet.Conn) (action gnet.Action) {
	wc, ok := c.Context().(*wsConn)
	if !ok {
		return gnet.Close
	}

	if !wc.upgraded {
		upgraded, err := h.tryUpgrade(c, wc)
		if err != nil {
			wc.setCloseErr(err)
			return gnet.Close
		}
		if !upgraded {
			return gnet.None
		}
	}

	for {
		hdr, payload, ok, err := wc.nextFrame()
		if err != nil {
			wc.setCloseErr(err)
			h.sendCloseOnError(wc, err)
			return gnet.Close
		}
		if !ok {
			return gnet.None
		}

		if hdr.OpCode.IsControl() {
			if h.handleControl(wc, hdr, payload) {
				return gnet.Close
			}
			continue
		}

		if err := h.handleData(wc, hdr, payload); err != nil {
			wc.setCloseErr(err)
			h.sendCloseOnError(wc, err)
			return gnet.Close
		}
	}
}

func (h *wsEventHandler) tryUpgrade(c gnet.Conn, wc *wsConn) (bool, error) {
	n := c.InboundBuffered()
	if n <= 0 {
		return false, nil
	}
	if n > maxHandshakeSize {
		return true, ws.ErrHandshakeBadProtocol
	}
	buf, err := c.Peek(n)
	if err != nil {
		return true, err
	}
	headerEnd := bytes.Index(buf, []byte("\r\n\r\n"))
	if headerEnd < 0 {
		return false, nil
	}
	reqBytes := buf[:headerEnd+4]

	var (
		reqURI  []byte
		reqHost string
		headers = make(http.Header)
	)

	upgrader := ws.Upgrader{
		ReadBufferSize:  h.server.opts.ReadBufferSize,
		WriteBufferSize: h.server.opts.WriteBufferSize,
		OnRequest: func(uri []byte) error {
			reqURI = append(reqURI[:0], uri...)
			if h.server.path == "" {
				return nil
			}
			u, err := url.ParseRequestURI(string(uri))
			if err != nil {
				return ws.RejectConnectionError(
					ws.RejectionStatus(http.StatusBadRequest),
					ws.RejectionReason("invalid request uri"),
				)
			}
			if u.Path != h.server.path {
				return ws.RejectConnectionError(
					ws.RejectionStatus(http.StatusNotFound),
					ws.RejectionReason("invalid websocket path"),
				)
			}
			return nil
		},
		OnHost: func(host []byte) error {
			reqHost = string(host)
			return nil
		},
		OnHeader: func(key, value []byte) error {
			headers.Add(string(key), string(value))
			return nil
		},
		OnBeforeUpgrade: func() (ws.HandshakeHeader, error) {
			if h.server.opts.WsOriginChecker == nil {
				return nil, nil
			}
			req := &http.Request{
				Method: http.MethodGet,
				Host:   reqHost,
				Header: headers,
			}
			if len(reqURI) > 0 {
				u, err := url.ParseRequestURI(string(reqURI))
				if err != nil {
					return nil, ws.RejectConnectionError(
						ws.RejectionStatus(http.StatusBadRequest),
						ws.RejectionReason("invalid request uri"),
					)
				}
				req.URL = u
				req.RequestURI = u.RequestURI()
			}
			if !h.server.opts.WsOriginChecker(req) {
				return nil, ws.RejectConnectionError(
					ws.RejectionStatus(http.StatusForbidden),
					ws.RejectionReason("forbidden origin"),
				)
			}
			return nil, nil
		},
	}

	rw := &upgradeReadWriter{
		r: bytes.NewReader(reqBytes),
		w: &gnetWriteAdapter{conn: c},
	}
	if _, err := upgrader.Upgrade(rw); err != nil {
		return true, err
	}
	_, _ = c.Discard(len(reqBytes))
	wc.upgraded = true

	h.server.connMgr.addConn(wc)
	h.server.stats.OnConnect()
	if h.server.handler != nil {
		kknet.SafeHandlerCall(h.server.opts.Logger, &h.server.stats, "kkwsgob OnConnect", func() {
			h.server.handler.OnConnect(wc)
		})
	}
	return true, nil
}

func (h *wsEventHandler) handleData(wc *wsConn, hdr ws.Header, payload []byte) error {
	switch hdr.OpCode {
	case ws.OpText, ws.OpBinary:
		if !hdr.Fin {
			wc.state = wc.state.Set(ws.StateFragmented)
			return wc.appendFragment(hdr.OpCode, payload)
		}
		msg := kkbuffer.GetWithCapacity(len(payload))
		msg.B = msg.B[:len(payload)]
		copy(msg.B, payload)
		h.server.stats.AddRecv(len(payload))
		h.server.dispatch(wc, msg)
		return nil
	case ws.OpContinuation:
		if wc.fragBuf == nil {
			return ws.ErrProtocolContinuationUnexpected
		}
		if err := wc.appendFragment(wc.fragOp, payload); err != nil {
			return err
		}
		if hdr.Fin {
			msg := wc.takeFragment()
			wc.state = wc.state.Clear(ws.StateFragmented)
			h.server.stats.AddRecv(len(msg.B))
			h.server.dispatch(wc, msg)
		}
		return nil
	default:
		return ws.ErrProtocolOpCodeReserved
	}
}

func (h *wsEventHandler) handleControl(wc *wsConn, hdr ws.Header, payload []byte) bool {
	switch hdr.OpCode {
	case ws.OpPing:
		wc.writeControl(ws.OpPong, payload)
		return false
	case ws.OpPong:
		return false
	case ws.OpClose:
		wc.writeControl(ws.OpClose, payload)
		return true
	default:
		return true
	}
}

func (h *wsEventHandler) sendCloseOnError(wc *wsConn, err error) {
	code := ws.StatusProtocolError
	if errors.Is(err, kkerrors.ErrMaxMessageSize) {
		code = ws.StatusMessageTooBig
	}
	payload := ws.NewCloseFrameBody(code, "")
	wc.writeControl(ws.OpClose, payload)
}

type gnetWriteAdapter struct {
	conn gnet.Conn
}

func (w *gnetWriteAdapter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	bb := kkbuffer.GetWithCapacity(len(p))
	bb.B = bb.B[:len(p)]
	copy(bb.B, p)
	if err := w.conn.AsyncWrite(bb.B, func(_ gnet.Conn, err error) error {
		kkbuffer.Put(bb)
		return nil
	}); err != nil {
		kkbuffer.Put(bb)
		return 0, err
	}
	return len(p), nil
}

type upgradeReadWriter struct {
	r *bytes.Reader
	w *gnetWriteAdapter
}

func (rw *upgradeReadWriter) Read(p []byte) (int, error) {
	return rw.r.Read(p)
}

func (rw *upgradeReadWriter) Write(p []byte) (int, error) {
	return rw.w.Write(p)
}

