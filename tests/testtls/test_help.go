package testtls

import (
	"net"
	"testing"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers"
)

type testHandler struct {
	onConnect func(kknet.IConn)
	onMessage func(kknet.IConn, []byte)
	onClose   func(kknet.IConn, error)
}

func (h *testHandler) OnConnect(c kknet.IConn) {
	if h.onConnect != nil {
		h.onConnect(c)
	}
}

func (h *testHandler) OnMessage(c kknet.IConn, data buffers.IBuffer) {
	if h.onMessage != nil {
		h.onMessage(c, bufferBytes(data))
	}
}

func (h *testHandler) OnClose(c kknet.IConn, err error) {
	if h.onClose != nil {
		h.onClose(c, err)
	}
}

func bufferBytes(data buffers.IBuffer) []byte {
	return append([]byte(nil), data.B...)
}

func freeTCPAddr(t testing.TB) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tcp: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}
