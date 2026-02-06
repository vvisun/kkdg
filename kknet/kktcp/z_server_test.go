package kktcp

import (
	"net"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func TestServer_NewServer(t *testing.T) {
	addr := "127.0.0.1:0"
	s := NewServer(addr, nil, kknet.DefaultOptions())
	if s == nil {
		t.Fatal("NewServer returned nil")
	}
	if s.Addr() != addr {
		t.Errorf("Addr() = %q, want %q", s.Addr(), addr)
	}
	if s.GetConnManager() == nil {
		t.Error("GetConnManager() returned nil")
	}
}

func TestServer_Start_Stop(t *testing.T) {
	addr := freePort(t)
	s := NewServer(addr, nil, kknet.DefaultOptions())
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	stats := s.Stats()
	if stats.ActiveConns != 0 {
		t.Errorf("ActiveConns = %d, want 0", stats.ActiveConns)
	}
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := s.Stop(); err != kkerrors.ErrServerNotStarted {
		t.Errorf("second Stop() = %v, want ErrServerNotStarted", err)
	}
}

func TestServer_Stop_WithoutStart(t *testing.T) {
	s := NewServer("127.0.0.1:0", nil, kknet.DefaultOptions())
	err := s.Stop()
	if err != kkerrors.ErrServerNotStarted {
		t.Errorf("Stop() = %v, want ErrServerNotStarted", err)
	}
}

func TestServer_Addr_Stats_GetConnManager(t *testing.T) {
	addr := freePort(t)
	s := NewServer(addr, nil, kknet.DefaultOptions())
	if s.Addr() != addr {
		t.Errorf("Addr() = %q, want %q", s.Addr(), addr)
	}
	mgr := s.GetConnManager()
	if mgr == nil {
		t.Fatal("GetConnManager() returned nil")
	}
	all := mgr.GetAllConns()
	if all == nil {
		t.Fatal("GetAllConns() returned nil")
	}
	if len(all) != 0 {
		t.Errorf("GetAllConns() len = %d, want 0", len(all))
	}
	_ = s.Start()
	defer s.Stop()
	ss := s.Stats()
	if ss.ActiveConns != 0 {
		t.Errorf("Stats().ActiveConns = %d, want 0", ss.ActiveConns)
	}
}

type testHandler struct {
	onConnect func(kknet.IConn)
	onClose   func(kknet.IConn, error)
}

func (h *testHandler) OnConnect(c kknet.IConn) {
	if h.onConnect != nil {
		h.onConnect(c)
	}
}
func (h *testHandler) OnClose(c kknet.IConn, err error) {
	if h.onClose != nil {
		h.onClose(c, err)
	}
}

func TestServer_ConnManager_GetConn_KickConn(t *testing.T) {
	addr := freePort(t)
	handler := &testHandler{
		onConnect: func(c kknet.IConn) {},
		onClose:   func(c kknet.IConn, err error) {},
	}
	s := NewServer(addr, handler, kknet.DefaultOptions())
	s.Start()
	defer s.Stop()

	client := NewClient(addr, nil, kknet.DefaultOptions())
	if err := client.Connect(); err != nil {
		t.Fatalf("client Connect: %v", err)
	}
	defer client.Close()

	time.Sleep(100 * time.Millisecond)
	mgr := s.GetConnManager()
	all := mgr.GetAllConns()
	if len(all) != 1 {
		t.Fatalf("GetAllConns() len = %d, want 1", len(all))
	}
	var connID int64
	for id := range all {
		connID = id
		break
	}
	conn := mgr.GetConn(connID)
	if conn == nil {
		t.Fatal("GetConn(id) returned nil")
	}
	if conn.ID() != connID {
		t.Errorf("GetConn(id).ID() = %d, want %d", conn.ID(), connID)
	}
	mgr.KickConn(connID)
	allAfter := mgr.GetAllConns()
	if _, ok := allAfter[connID]; ok {
		t.Errorf("GetAllConns() still contains connID %d after KickConn", connID)
	}
}

func TestServer_Stop_ClosesConnections(t *testing.T) {
	addr := freePort(t)
	closed := make(chan struct{})
	handler := &testHandler{
		onClose: func(c kknet.IConn, err error) { close(closed) },
	}
	s := NewServer(addr, handler, kknet.DefaultOptions())
	s.Start()

	client := NewClient(addr, nil, kknet.DefaultOptions())
	if err := client.Connect(); err != nil {
		t.Fatalf("client Connect: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("client connection not closed after Stop()")
	}
	_ = client.Close()
}

func TestServerClient_Integration_Echo(t *testing.T) {
	addr := freePort(t)
	recvCh := make(chan []byte, 16)
	var serverConn kknet.IConn
	echoHandler := &tcpEchoHandler{
		onConnect: func(c kknet.IConn) { serverConn = c },
		onRaw:     func(data []byte) { recvCh <- data },
	}
	opts := kknet.ApplyOptions(kknet.WithRawHandler(echoHandler))
	s := NewServer(addr, echoHandler, opts)
	s.Start()
	defer s.Stop()

	client := NewClient(addr, nil, kknet.DefaultOptions())
	if err := client.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Close()
	time.Sleep(100 * time.Millisecond)
	if serverConn == nil {
		t.Fatal("server did not get connection")
	}

	payload := []byte("echo test")
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

type tcpEchoHandler struct {
	onConnect func(kknet.IConn)
	onRaw     func(data []byte)
}

func (h *tcpEchoHandler) OnConnect(c kknet.IConn) {
	if h.onConnect != nil {
		h.onConnect(c)
	}
}
func (h *tcpEchoHandler) OnClose(c kknet.IConn, err error) {}
func (h *tcpEchoHandler) OnRaw(connID int64, data *kkbuffer.ByteBuffer) {
	if h.onRaw != nil && data != nil {
		b := append([]byte(nil), data.Bytes()...)
		h.onRaw(b)
	}
	// ReadProcessor releases data after OnRaw returns
}
