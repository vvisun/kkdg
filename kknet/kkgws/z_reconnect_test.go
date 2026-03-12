package kkgws

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet"
)

type testLifecycleHandler struct {
	onConnectCh chan struct{}
	onCloseCh   chan struct{}
	connects    atomic.Int32
	closes      atomic.Int32
}

func (h *testLifecycleHandler) OnConnect(_ kknet.IConn) {
	h.connects.Add(1)
	select {
	case h.onConnectCh <- struct{}{}:
	default:
	}
}

func (h *testLifecycleHandler) OnClose(_ kknet.IConn, _ error) {
	h.closes.Add(1)
	select {
	case h.onCloseCh <- struct{}{}:
	default:
	}
}

// TestKKGWS_Client_Reconnect 服务端 KickConn 断开首连后，客户端自动重连并能正常收发。
func TestKKGWS_Client_Reconnect(t *testing.T) {
	addr := freePort(t)
	recvCh := make(chan []byte, 16)
	var serverConnID kknet.CONN_ID
	echoHandler := &echoHandler{
		onConnect: func(c kknet.IConn) { serverConnID = c.ID() },
		onRaw:     func(data []byte) { recvCh <- data },
	}
	opts := kknet.ApplyOptions(kknet.WithRawHandler(echoHandler))
	s := NewServer(addr, echoHandler, opts)
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer s.Stop()

	h := &testLifecycleHandler{
		onConnectCh: make(chan struct{}, 8),
		onCloseCh:   make(chan struct{}, 8),
	}
	clientOpts := kknet.ApplyOptions(
		kknet.WithRawHandler(&noopRawHandler{}),
		kknet.WithIsNeedReconnect(true),
		kknet.WithReconnectInterval(50*time.Millisecond, 20),
		kknet.WithReconnectCallback(func(_ int, _ error) {}),
		kknet.WithSendQueueSize(64),
	)
	client := NewClient("ws://"+addr+"/ws", h, clientOpts)
	if err := client.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Close()

	// 等待首次连接
	select {
	case <-h.onConnectCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for first OnConnect")
	}
	time.Sleep(100 * time.Millisecond)
	// 服务端踢掉当前连接，触发客户端重连
	mgr := s.GetConnManager()
	mgr.KickConn(serverConnID)

	// 等待重连后的 OnConnect
	select {
	case <-h.onConnectCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for reconnect OnConnect")
	}

	// 重连后应能正常发送
	bb, err := opts.StreamTool.Pack([]byte("hi"))
	if err != nil {
		t.Fatalf("Pack: %v", err)
	}
	if err := client.SendBuffer(bb); err != nil {
		t.Fatalf("SendBuffer: %v", err)
	}
	select {
	case frame := <-recvCh:
		msg, err := opts.StreamTool.Unpack(frame)
		if err != nil {
			t.Fatalf("Unpack: %v", err)
		}
		if string(msg) != "hi" {
			t.Fatalf("server got %q, want %q", string(msg), "hi")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for server recv")
	}

	_ = client.Close()
	prevConnects := h.connects.Load()
	time.Sleep(200 * time.Millisecond)
	if h.connects.Load() != prevConnects {
		t.Fatalf("unexpected reconnect after Close")
	}
}
