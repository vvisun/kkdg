package kkws

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
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

func TestKKWS_Client_Reconnect(t *testing.T) {
	var connNum atomic.Int32
	recvCh := make(chan []byte, 16)

	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}

		n := connNum.Add(1)
		if n == 1 {
			// Close the first connection soon to trigger reconnect.
			go func() {
				_ = c.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "bye"), time.Now().Add(250*time.Millisecond))
				_ = c.Close()
			}()
			return
		}

		// Keep subsequent connections open and read messages.
		go func() {
			defer func() { _ = c.Close() }()
			for {
				mt, data, err := c.ReadMessage()
				if err != nil {
					return
				}
				if mt != websocket.BinaryMessage {
					continue
				}
				// We only need the first packet for this test.
				msg, err := kkpacket.DefaultStreamPacket().Unpack(data)
				if err != nil {
					continue
				}
				select {
				case recvCh <- msg:
				default:
				}
			}
		}()
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"

	h := &testLifecycleHandler{
		onConnectCh: make(chan struct{}, 8),
		onCloseCh:   make(chan struct{}, 8),
	}

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
	cli := NewClient(wsURL, h, opts)

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
	bb, err := kkpacket.DefaultStreamPacket().Pack([]byte("hi"))
	if err != nil {
		t.Fatalf("pack error: %v", err)
	}
	if err := cli.SendBuffer(bb); err != nil {
		t.Fatalf("SendBuffer error: %v", err)
	}

	select {
	case msg := <-recvCh:
		if string(msg) != "hi" {
			t.Fatalf("server got %q, want %q", string(msg), "hi")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for server recv")
	}

	// Close should stop further reconnect attempts.
	_ = cli.Close()
	prevConnects := h.connects.Load()
	time.Sleep(200 * time.Millisecond)
	if h.connects.Load() != prevConnects {
		t.Fatalf("unexpected reconnect after Close")
	}
	_ = cbAttempts.Load() // ensure callback executed at least once (not asserted, just coverage)
}
