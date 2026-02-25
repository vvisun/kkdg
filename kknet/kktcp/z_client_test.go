package kktcp

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
	}
	// ReadProcessor releases data after OnRaw returns
	kkbuffer.Put(data)
}

// noopRawHandler satisfies RawHandler for tests that only need Connect/Close.
type noopRawHandler struct{}

func (h *noopRawHandler) OnRaw(_ kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	kkbuffer.Put(data)
}

func TestClient_NewClient(t *testing.T) {
	c := NewClient("127.0.0.1:8080", nil, kknet.DefaultOptions())
	if c == nil {
		t.Fatal("NewClient returned nil")
	}
	if c.Addr() != "127.0.0.1:8080" {
		t.Errorf("Addr() = %q, want 127.0.0.1:8080", c.Addr())
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
	s.Start()
	defer s.Stop()

	client := NewClient(addr, nil, opts)
	if err := client.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	stats := client.Stats()
	if stats.ActiveConns != 1 {
		t.Errorf("Stats().ActiveConns = %d, want 1", stats.ActiveConns)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// kktcp 在 Close 后 gnet client 已停止，再次 Connect 会返回 ErrClientNotConnected
	_ = client.Connect()
	if err := client.Close(); err != kkerrors.ErrClientNotConnected {
		t.Errorf("Close when not connected = %v, want ErrClientNotConnected", err)
	}
}

func TestClient_SendBuffer(t *testing.T) {
	addr := freePort(t)
	recvCh := make(chan []byte, 16)
	opts := kknet.ApplyOptions(kknet.WithRawHandler(&rawRecvHandler{ch: recvCh}))
	s := NewServer(addr, nil, opts)
	s.Start()
	defer s.Stop()

	client := NewClient(addr, nil, opts)
	if err := client.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Close()
	time.Sleep(100 * time.Millisecond)

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
	client := NewClient("127.0.0.1:19999", nil, kknet.DefaultOptions())
	bb, _ := kkpacket.DefaultStreamPacket().Pack([]byte("x"))
	err := client.SendBuffer(bb)
	if err != kkerrors.ErrClientNotConnected {
		t.Errorf("SendBuffer when not connected = %v, want ErrClientNotConnected", err)
	}
}
