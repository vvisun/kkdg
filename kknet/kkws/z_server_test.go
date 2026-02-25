package kkws

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
	if s.path != "/ws" {
		t.Errorf("default path = %q, want /ws", s.path)
	}
}

func TestServer_SetPath(t *testing.T) {
	s := NewServer("127.0.0.1:0", nil, kknet.DefaultOptions())
	s.SetPath("/custom")
	if s.path != "/custom" {
		t.Errorf("path = %q, want /custom", s.path)
	}
	s.SetPath("")
	if s.path != "/custom" {
		t.Errorf("SetPath(\"\") should not change path, got %q", s.path)
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
	// second Stop returns ErrServerNotStarted
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
	var count int
	mgr.RangeAllConns(func(id kknet.CONN_ID, conn kknet.IConn) bool {
		count++
		return true
	})
	if count != 0 {
		t.Errorf("RangeAllConns() count = %d, want 0", count)
	}
	_ = s.Start()
	defer s.Stop()
	ss := s.Stats()
	if ss.ActiveConns != 0 {
		t.Errorf("Stats().ActiveConns = %d, want 0", ss.ActiveConns)
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

	client := NewClient("ws://"+addr+"/ws", nil, kknet.DefaultOptions())
	if err := client.Connect(); err != nil {
		t.Fatalf("client Connect: %v", err)
	}
	defer client.Close()

	// wait for server to see the connection
	time.Sleep(100 * time.Millisecond)
	mgr := s.GetConnManager()
	var count int
	mgr.RangeAllConns(func(id kknet.CONN_ID, conn kknet.IConn) bool {
		count++
		return true
	})
	if count != 1 {
		t.Fatalf("RangeAllConns() count = %d, want 1", count)
	}
	var connID kknet.CONN_ID
	mgr.RangeAllConns(func(id kknet.CONN_ID, conn kknet.IConn) bool {
		connID = id
		return false
	})
	conn := mgr.GetConn(connID)
	if conn == nil {
		t.Fatal("GetConn(id) returned nil")
	}
	if conn.ID() != connID {
		t.Errorf("GetConn(id).ID() = %d, want %d", conn.ID(), connID)
	}
	// kick connection: Close and remove from manager
	mgr.KickConn(connID)
	// after KickConn, connection is removed from manager
	var countAfter int
	mgr.RangeAllConns(func(id kknet.CONN_ID, conn kknet.IConn) bool {
		count++
		return true
	})
	if countAfter != 0 {
		t.Fatalf("RangeAllConns() count = %d, want 0 after KickConn", count)
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

func TestServer_Stop_ClosesConnections(t *testing.T) {
	addr := freePort(t)
	closed := make(chan struct{})
	handler := &testHandler{
		onClose: func(c kknet.IConn, err error) { close(closed) },
	}
	s := NewServer(addr, handler, kknet.DefaultOptions())
	s.Start()

	client := NewClient("ws://"+addr+"/ws", nil, kknet.DefaultOptions())
	if err := client.Connect(); err != nil {
		t.Fatalf("client Connect: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("client connection not closed after Stop()")
	}
	_ = client.Close()
}

func TestServerClient_Integration_Echo(t *testing.T) {
	addr := freePort(t)
	recvCh := make(chan []byte, 16)
	var serverConn kknet.IConn
	echoHandler := &echoHandler{
		onConnect: func(c kknet.IConn) { serverConn = c },
		onRaw:     func(data []byte) { recvCh <- data },
	}
	opts := kknet.ApplyOptions(kknet.WithRawHandler(echoHandler))
	s := NewServer(addr, echoHandler, opts)
	s.Start()
	defer s.Stop()

	client := NewClient("ws://"+addr+"/ws", nil, kknet.DefaultOptions())
	if err := client.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Close()
	time.Sleep(50 * time.Millisecond)
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

// echoHandler implements both INewHandler and IRawHandler for integration test.
type echoHandler struct {
	onConnect func(kknet.IConn)
	onRaw     func(data []byte)
}

func (h *echoHandler) OnConnect(c kknet.IConn) {
	if h.onConnect != nil {
		h.onConnect(c)
	}
}
func (h *echoHandler) OnClose(c kknet.IConn, err error) {}
func (h *echoHandler) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	if h.onRaw != nil && data != nil {
		b := append([]byte(nil), data.Bytes()...)
		h.onRaw(b)
		kkbuffer.Put(data)
	}
}

// TestPingPong_Keepalive 演示 Ping/Pong 保活：读超时 4s，Ping 间隔 1.5s；空闲 5s 后仍能收发，说明 Pong 刷新了读超时。
//
// 使用方式：服务端/客户端均设置 WithWsReadTimeout + WithWsPingInterval，例如：
//
//	opts := kknet.ApplyOptions(
//	    kknet.WithWsReadTimeout(30*time.Second),
//	    kknet.WithWsPingInterval(10*time.Second),
//	)
//	server := kkws.NewServer(addr, handler, opts)
//	client := kkws.NewClient(url, handler, opts)
func TestPingPong_Keepalive(t *testing.T) {
	addr := freePort(t)
	recvCh := make(chan []byte, 4)
	opts := kknet.ApplyOptions(
		kknet.WithReadTimeout(4*time.Second),
		kknet.WithPingInterval(1500*time.Millisecond),
		kknet.WithRawHandler(&rawRecvHandlerForPingTest{ch: recvCh}),
	)
	s := NewServer(addr, nil, opts)
	s.Start()
	defer s.Stop()

	clientOpts := kknet.ApplyOptions(
		kknet.WithReadTimeout(4*time.Second),
		kknet.WithPingInterval(1500*time.Millisecond),
	)
	client := NewClient("ws://"+addr+"/ws", nil, clientOpts)
	if err := client.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Close()
	time.Sleep(100 * time.Millisecond)

	// 空闲超过读超时（4s），仅靠 Ping/Pong 保活
	time.Sleep(5 * time.Second)

	// 仍能正常收发说明连接未因读超时断开
	payload := []byte("alive")
	bb, err := kkpacket.DefaultStreamPacket().Pack(payload)
	if err != nil {
		t.Fatalf("Pack: %v", err)
	}
	if err := client.SendBuffer(bb); err != nil {
		t.Fatalf("SendBuffer after idle: %v", err)
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
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for message after idle (Ping/Pong keepalive may not be active)")
	}
}

type rawRecvHandlerForPingTest struct {
	ch chan []byte
}

func (h *rawRecvHandlerForPingTest) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	if data == nil {
		return
	}
	b := append([]byte(nil), data.Bytes()...)
	select {
	case h.ch <- b:
	default:
	}
	kkbuffer.Put(data)
}
