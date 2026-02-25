package kkudp

import (
	"testing"
	"time"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

type udpRawRecvHandler struct {
	ch chan []byte
}

func (h *udpRawRecvHandler) OnConnect(kknet.IConn) {}
func (h *udpRawRecvHandler) OnClose(kknet.IConn, error) {}
func (h *udpRawRecvHandler) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	if data != nil {
		b := append([]byte(nil), data.Bytes()...)
		select {
		case h.ch <- b:
		default:
		}
	}
	kkbuffer.Put(data)
}

func TestClient_NewClient(t *testing.T) {
	c := NewClient("127.0.0.1:9999", nil, kknet.DefaultOptions())
	if c == nil {
		t.Fatal("NewClient returned nil")
	}
	if c.Addr() != "127.0.0.1:9999" {
		t.Errorf("Addr() = %q, want 127.0.0.1:9999", c.Addr())
	}
}

func TestClient_Connect_Close(t *testing.T) {
	addr := freeUDPPort(t)
	s := NewServer(addr, nil, kknet.DefaultOptions())
	if err := s.Start(); err != nil {
		t.Fatalf("Server Start: %v", err)
	}
	defer s.Stop()

	client := NewClient(addr, nil, kknet.DefaultOptions())
	if err := client.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if !client.IsConnected() {
		t.Error("IsConnected() should be true")
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := client.Close(); err != kkerrors.ErrClientNotConnected {
		t.Errorf("Close when not connected = %v, want ErrClientNotConnected", err)
	}
}

func TestClient_SendBuffer_Echo(t *testing.T) {
	addr := freeUDPPort(t)
	recvCh := make(chan []byte, 16)
	echoHandler := &udpEchoHandler{}
	clientRecv := &udpRawRecvHandler{ch: recvCh}

	opts := kknet.ApplyOptions(kknet.WithRawHandler(echoHandler))
	s := NewServer(addr, echoHandler, opts)
	echoHandler.server = s
	if err := s.Start(); err != nil {
		t.Fatalf("Server Start: %v", err)
	}
	defer s.Stop()

	client := NewClient(addr, clientRecv, kknet.ApplyOptions(kknet.WithRawHandler(clientRecv)))
	if err := client.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Close()

	time.Sleep(100 * time.Millisecond)

	payload := []byte("hello udp")
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
		t.Fatal("timeout waiting for echo")
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

type udpEchoHandler struct {
	server kknet.IServer
}

func (h *udpEchoHandler) OnConnect(c kknet.IConn) {}
func (h *udpEchoHandler) OnClose(c kknet.IConn, err error) {}
func (h *udpEchoHandler) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	if data == nil || h.server == nil {
		if data != nil {
			kkbuffer.Put(data)
		}
		return
	}
	bb := kkbuffer.GetWithCapacity(len(data.Bytes()))
	bb.B = append(bb.B[:0], data.Bytes()...)
	kkbuffer.Put(data)
	_ = h.server.SendBuffer(connID, bb)
}
