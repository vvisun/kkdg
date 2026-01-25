package kkwsgob

import (
	"net"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers"
)

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

type gobTestHandler struct {
	onConnect func(kknet.IConn)
	onMessage func(kknet.IConn, []byte)
	onClose   func(kknet.IConn, error)
}

func (h *gobTestHandler) OnConnect(c kknet.IConn) {
	if h.onConnect != nil {
		h.onConnect(c)
	}
}

func (h *gobTestHandler) OnMessage(c kknet.IConn, data buffers.IBuffer) {
	if h.onMessage != nil {
		b := append([]byte(nil), data.B...)
		h.onMessage(c, b)
	}
}

func (h *gobTestHandler) OnClose(c kknet.IConn, err error) {
	if h.onClose != nil {
		h.onClose(c, err)
	}
}

func TestKKWSGobRoundtrip(t *testing.T) {
	addr := freeTCPAddr(t)

	serverHandler := &gobTestHandler{
		onMessage: func(c kknet.IConn, data []byte) {
			_ = c.Send(data)
		},
	}
	server := NewServer(addr, serverHandler)
	server.SetPath("/ws")

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Start()
	}()
	defer func() { _ = server.Stop() }()

	clientHandler := &gobTestHandler{}
	client := NewClient("ws://"+addr+"/ws", clientHandler)

	deadline := time.Now().Add(3 * time.Second)
	for {
		err := client.Connect()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("client connect: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	defer func() { _ = client.Close() }()

	replyCh := make(chan []byte, 1)
	clientHandler.onMessage = func(c kknet.IConn, data []byte) {
		replyCh <- data
	}

	payload := []byte("ping")
	if err := client.Send(payload); err != nil {
		t.Fatalf("client send: %v", err)
	}

	select {
	case got := <-replyCh:
		if string(got) != string(payload) {
			t.Fatalf("unexpected ws reply: %s", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ws reply timeout")
	}

	srvStats := server.Stats()
	if srvStats.RecvMsgs == 0 || srvStats.SentMsgs == 0 {
		t.Fatalf("server stats not updated: %+v", srvStats)
	}
	cliStats := client.Stats()
	if cliStats.RecvMsgs == 0 || cliStats.SentMsgs == 0 {
		t.Fatalf("client stats not updated: %+v", cliStats)
	}
}

func TestKKWSGobSetPath(t *testing.T) {
	addr := freeTCPAddr(t)

	serverHandler := &gobTestHandler{
		onMessage: func(c kknet.IConn, data []byte) {
			_ = c.Send(data)
		},
	}
	server := NewServer(addr, serverHandler)
	server.SetPath("/custom")

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Start()
	}()
	defer func() { _ = server.Stop() }()

	clientHandler := &gobTestHandler{}
	client := NewClient("ws://"+addr+"/custom", clientHandler)

	deadline := time.Now().Add(3 * time.Second)
	for {
		err := client.Connect()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("client connect: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	defer func() { _ = client.Close() }()

	replyCh := make(chan []byte, 1)
	clientHandler.onMessage = func(c kknet.IConn, data []byte) {
		replyCh <- data
	}

	payload := []byte("path-test")
	if err := client.Send(payload); err != nil {
		t.Fatalf("client send: %v", err)
	}

	select {
	case got := <-replyCh:
		if string(got) != string(payload) {
			t.Fatalf("unexpected reply: %s", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("reply timeout")
	}
}
