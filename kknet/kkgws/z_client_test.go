package kkgws

import (
	"testing"
	"time"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

type rawRecvHandler struct {
	ch chan []byte
}

func (h *rawRecvHandler) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	if data != nil {
		b := append([]byte(nil), data.Bytes()...)
		select {
		case h.ch <- b:
		default:
		}
		kkbuffer.Put(data)
	}
}

func TestClient_NewClient(t *testing.T) {
	c := NewClient("ws://127.0.0.1:8080/ws", nil, kknet.DefaultOptions())
	if c == nil {
		t.Fatal("NewClient returned nil")
	}
	if c.Addr() != "ws://127.0.0.1:8080/ws" {
		t.Errorf("Addr() = %q, want ws://127.0.0.1:8080/ws", c.Addr())
	}
	stats := c.Stats()
	if stats.ActiveConns != 0 {
		t.Errorf("Stats().ActiveConns = %d, want 0", stats.ActiveConns)
	}
}

func TestClient_Connect_Close(t *testing.T) {
	addr := freePort(t)
	opts := kknet.ApplyOptions(kknet.WithRawHandler(&noopRawHandler{}))
	s := NewServer(addr, nil, opts)
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer s.Stop()

	client := NewClient("ws://"+addr+"/ws", nil, opts)
	if err := client.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if client.Conn() == nil {
		t.Fatal("Conn() after Connect is nil")
	}
	stats := client.Stats()
	if stats.ActiveConns != 1 {
		t.Errorf("Stats().ActiveConns = %d, want 1", stats.ActiveConns)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := client.Connect(); err != nil {
		t.Fatalf("second Connect: %v", err)
	}
	_ = client.Close()
	if err := client.Close(); err != kkerrors.ErrNetClientNotConnected {
		t.Errorf("Close when not connected = %v, want ErrClientNotConnected", err)
	}
}

func TestClient_SendBuffer(t *testing.T) {
	addr := freePort(t)
	recvCh := make(chan []byte, 16)
	opts := kknet.ApplyOptions(kknet.WithRawHandler(&rawRecvHandler{ch: recvCh}))
	s := NewServer(addr, nil, opts)
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer s.Stop()

	client := NewClient("ws://"+addr+"/ws", nil, opts)
	if err := client.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Close()
	time.Sleep(50 * time.Millisecond)

	payload := []byte("hello")
	bb, err := kkpacket.DefaultStreamPacket().Pack(payload)
	if err != nil {
		t.Fatalf("Pack: %v", err)
	}
	if err := client.SendBuffer(bb); err != nil {
		t.Fatalf("SendBuffer: %v", err)
	}
	select {
	case got := <-recvCh:
		msg, err := kkpacket.DefaultStreamPacket().Unpack(got)
		if err != nil {
			t.Fatalf("Unpack: %v", err)
		}
		if string(msg) != string(payload) {
			t.Errorf("got %q, want %q", msg, payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for server to receive message")
	}
}

func TestClient_SendBuffer_NotConnected(t *testing.T) {
	client := NewClient("ws://127.0.0.1:9999/ws", nil, kknet.DefaultOptions())
	bb, _ := kkpacket.DefaultStreamPacket().Pack([]byte("x"))
	err := client.SendBuffer(bb)
	if err != kkerrors.ErrNetClientNotConnected {
		t.Errorf("SendBuffer when not connected = %v, want ErrClientNotConnected", err)
	}
}

func TestClient_Conn_Stats_Addr(t *testing.T) {
	addr := freePort(t)
	opts := kknet.ApplyOptions(kknet.WithRawHandler(&noopRawHandler{}))
	s := NewServer(addr, nil, opts)
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer s.Stop()

	url := "ws://" + addr + "/ws"
	client := NewClient(url, nil, opts)
	if client.Addr() != url {
		t.Errorf("Addr() = %q, want %q", client.Addr(), url)
	}
	client.Connect()
	defer client.Close()
	time.Sleep(50 * time.Millisecond)
	conn := client.Conn()
	if conn == nil {
		t.Fatal("Conn() is nil")
	}
	if conn.RemoteAddr() == "" {
		t.Error("RemoteAddr() is empty")
	}
	stats := client.Stats()
	if stats.ActiveConns != 1 {
		t.Errorf("Stats().ActiveConns = %d, want 1", stats.ActiveConns)
	}
}
