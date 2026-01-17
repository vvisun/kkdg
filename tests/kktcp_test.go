package tests

import (
	"net"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/utils/buffers"
)

type testHandler struct {
	onConnect func(kknet.Conn)
	onMessage func(kknet.Conn, []byte)
	onClose   func(kknet.Conn, error)
}

func (h *testHandler) OnConnect(c kknet.Conn) {
	if h.onConnect != nil {
		h.onConnect(c)
	}
}

func (h *testHandler) OnMessage(c kknet.Conn, data buffers.IBuffer) {
	if h.onMessage != nil {
		h.onMessage(c, bufferBytes(data))
	}
}

func (h *testHandler) OnClose(c kknet.Conn, err error) {
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

func TestKKNetTCP(t *testing.T) {
	addr := freeTCPAddr(t)

	serverHandler := &testHandler{
		onMessage: func(c kknet.Conn, data []byte) {
			_ = c.Send(data)
		},
	}
	server := kktcp.NewServer(addr, serverHandler)
	if err := server.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer func() { _ = server.Stop() }()

	msgCh := make(chan []byte, 1)
	clientHandler := &testHandler{
		onMessage: func(c kknet.Conn, data []byte) {
			msgCh <- data
		},
	}
	client := kktcp.NewClient(addr, clientHandler)
	if err := client.Connect(); err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer func() { _ = client.Close() }()

	payload := []byte("ping")
	if err := client.Send(payload); err != nil {
		t.Fatalf("client send: %v", err)
	}

	select {
	case got := <-msgCh:
		if string(got) != string(payload) {
			t.Fatalf("unexpected tcp reply: %s", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("tcp reply timeout")
	}
}
