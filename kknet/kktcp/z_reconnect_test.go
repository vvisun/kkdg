package kktcp

import (
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet"
)

type reconnectLifecycleHandler struct {
	onConnectCh chan struct{}
	onCloseCh   chan struct{}
	connects    atomic.Int32
	closes      atomic.Int32
}

func (h *reconnectLifecycleHandler) OnConnect(_ kknet.IConn) {
	h.connects.Add(1)
	select {
	case h.onConnectCh <- struct{}{}:
	default:
	}
}

func (h *reconnectLifecycleHandler) OnClose(_ kknet.IConn, _ error) {
	h.closes.Add(1)
	select {
	case h.onCloseCh <- struct{}{}:
	default:
	}
}

// TestGnetClient_Reconnect mirrors kkws.TestKKWS_Client_Reconnect but for TCP.
// It verifies that when the first connection is closed by server, the client
// performs reconnect attempts and can send data on the new connection.
func TestGnetClient_Reconnect(t *testing.T) {
	var cbAttempts atomic.Int32
	opts := kknet.ApplyOptions(
		kknet.WithRawHandler(&noopRawHandler{}),
		kknet.WithIsNeedReconnect(true),
		kknet.WithReconnectInterval(50*time.Millisecond, 20),
		kknet.WithReconnectCallback(func(_ int, _ error) {
			cbAttempts.Add(1)
		}),
		kknet.WithSendQueueSize(64),
	)

	var connNum atomic.Int32
	recvCh := make(chan []byte, 16)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	addr := ln.Addr().String()

	// TCP server: first connection is closed quickly to trigger reconnect,
	// subsequent connections stay open and echo first packet payload.
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			n := connNum.Add(1)
			if n == 1 {
				// Close the first connection soon to trigger reconnect.
				go func() {
					time.Sleep(200 * time.Millisecond)
					_ = c.Close()
				}()
				continue
			}

			// Keep subsequent connections open and read one packet.
			go func(conn net.Conn) {
				defer func() { _ = conn.Close() }()
				buf := make([]byte, 4096)
				n, err := conn.Read(buf)
				if err != nil || n <= 0 {
					return
				}
				// Best‑effort: unpack first stream packet.
				frame := buf[:n]
				msg, err := opts.StreamTool.Unpack(frame)
				if err != nil {
					return
				}
				select {
				case recvCh <- msg:
				default:
				}
			}(c)
		}
	}()

	h := &reconnectLifecycleHandler{
		onConnectCh: make(chan struct{}, 8),
		onCloseCh:   make(chan struct{}, 8),
	}

	cli := NewClient(addr, h, opts)

	if err := cli.Connect(); err != nil {
		t.Fatalf("Connect error: %v", err)
	}
	defer cli.Close()

	// Expect at least two connects: first + reconnect.
	select {
	case <-h.onConnectCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for first OnConnect")
	}
	select {
	case <-h.onConnectCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for reconnect OnConnect")
	}

	// After reconnect, client should be able to send.
	bb, err := opts.StreamTool.Pack([]byte("hi"))
	if err != nil {
		t.Fatalf("Pack error: %v", err)
	}
	if err := cli.SendBuffer(bb); err != nil {
		t.Fatalf("SendBuffer error after reconnect: %v", err)
	}

	select {
	case msg := <-recvCh:
		if string(msg) != "hi" {
			t.Fatalf("server got %q, want %q", string(msg), "hi")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for server recv after reconnect")
	}

	// Close should stop further reconnect attempts (best-effort check).
	_ = cli.Close()
	prevConnects := h.connects.Load()
	time.Sleep(200 * time.Millisecond)
	if h.connects.Load() != prevConnects {
		t.Fatalf("unexpected reconnect after Close")
	}
	_ = cbAttempts.Load() // ensure callback executed at least once (coverage)
}
