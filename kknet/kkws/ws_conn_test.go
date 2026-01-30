package kkws

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
)

func startTestWSServer(t *testing.T) (wsURL string, recv <-chan []byte, closeFn func()) {
	t.Helper()

	recvCh := make(chan []byte, 1024)

	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Logf("upgrade error: %v", err)
			return
		}

		go func() {
			defer func() { _ = conn.Close() }()
			for {
				mt, data, err := conn.ReadMessage()
				if err != nil {
					return
				}
				if mt != websocket.BinaryMessage {
					continue
				}
				// One ws message may contain multiple stream packets: [len,msg][len,msg]...
				pos := 0
				for pos < len(data) {
					if len(data)-pos < kkpacket.DefaultStreamPacket().LengthFieldByteCount() {
						break
					}
					size, err := kkpacket.DefaultStreamPacket().GetBodySize(data[pos:])
					if err != nil {
						break
					}
					totalLen := kkpacket.DefaultStreamPacket().LengthFieldByteCount() + size
					if len(data)-pos < totalLen {
						break
					}
					cp := make([]byte, totalLen)
					copy(cp, data[pos:pos+totalLen])
					select {
					case recvCh <- cp:
					default:
						// avoid blocking server read loop
					}
					pos += totalLen
				}
			}
		}()
	})

	srv := httptest.NewServer(mux)
	wsURL = "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"

	return wsURL, recvCh, func() {
		srv.Close()
	}
}

func mustRecv(t *testing.T, ch <-chan []byte, timeout time.Duration) []byte {
	t.Helper()
	select {
	case b := <-ch:
		return b
	case <-time.After(timeout):
		t.Fatalf("timeout waiting for ws message after %v", timeout)
		return nil
	}
}

func TestWSConn_AsyncSend_Order(t *testing.T) {
	wsURL, recv, closeSrv := startTestWSServer(t)
	defer closeSrv()

	c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}

	opts := kknet.ApplyOptions()
	wc := newWSConn(c, opts, nil)
	defer wc.Close()

	const n = 50
	for i := 0; i < n; i++ {
		payload := []byte(fmt.Sprintf("m%d", i))
		bb, err := kkpacket.DefaultStreamPacket().Pack(payload)
		if err != nil {
			t.Fatalf("pack error: %v", err)
		}
		if err := wc.SendBuffer(bb); err != nil {
			t.Fatalf("SendBuffer error: %v", err)
		}
	}

	for i := 0; i < n; i++ {
		frame := mustRecv(t, recv, 2*time.Second)
		msg, err := kkpacket.DefaultStreamPacket().Unpack(frame)
		if err != nil {
			t.Fatalf("unpack error: %v", err)
		}
		want := fmt.Sprintf("m%d", i)
		if string(msg) != want {
			t.Fatalf("got %q, want %q", string(msg), want)
		}
	}
}

func TestWSConn_SendQueueStrict_Full(t *testing.T) {
	wsURL, recv, closeSrv := startTestWSServer(t)
	defer closeSrv()

	c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}

	opts := kknet.ApplyOptions(kknet.WithSendQueueSize(2))
	opts.SendQueueStrict = true
	wc := newWSConn(c, opts, nil)
	defer wc.Close()

	// Block actual writes so the queue can fill deterministically.
	wc.writeMu.Lock()

	for i := 0; i < 2; i++ {
		bb, err := kkpacket.DefaultStreamPacket().Pack([]byte(fmt.Sprintf("x%d", i)))
		if err != nil {
			t.Fatalf("pack error: %v", err)
		}
		if err := wc.SendBuffer(bb); err != nil {
			t.Fatalf("SendBuffer error: %v", err)
		}
	}

	bb, err := kkpacket.DefaultStreamPacket().Pack([]byte("overflow"))
	if err != nil {
		t.Fatalf("pack error: %v", err)
	}
	if err := wc.SendBuffer(bb); err == nil {
		t.Fatalf("expected ErrSendQueueFull, got nil")
	} else if err != kkerrors.ErrSendQueueFull {
		t.Fatalf("expected ErrSendQueueFull, got %v", err)
	}

	// Unblock writer and ensure only 2 messages are delivered.
	wc.writeMu.Unlock()

	for i := 0; i < 2; i++ {
		_ = mustRecv(t, recv, 2*time.Second)
	}
	select {
	case b := <-recv:
		t.Fatalf("unexpected extra message: %v", b)
	case <-time.After(150 * time.Millisecond):
	}
}

func TestWSConn_Close_FlushOver(t *testing.T) {
	wsURL, recv, closeSrv := startTestWSServer(t)
	defer closeSrv()

	c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}

	opts := kknet.ApplyOptions(kknet.WithSendQueueSize(64))
	opts.SendQueueNeedFlushOver = true
	opts.SendQueueTimeoutFlushOver = 2 * time.Second
	wc := newWSConn(c, opts, nil)

	// Block actual writes, enqueue some messages, then ensure Close waits for flush.
	wc.writeMu.Lock()
	const n = 10
	for i := 0; i < n; i++ {
		bb, err := kkpacket.DefaultStreamPacket().Pack([]byte(fmt.Sprintf("f%d", i)))
		if err != nil {
			t.Fatalf("pack error: %v", err)
		}
		if err := wc.SendBuffer(bb); err != nil {
			t.Fatalf("SendBuffer error: %v", err)
		}
	}

	closed := make(chan struct{})
	go func() {
		_ = wc.Close()
		close(closed)
	}()

	// Close should be blocked while writeMu is held (writer can't flush).
	select {
	case <-closed:
		t.Fatalf("Close returned early; expected it to wait for flush")
	case <-time.After(150 * time.Millisecond):
	}

	// Allow flush.
	wc.writeMu.Unlock()

	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatalf("Close did not return after flush")
	}

	for i := 0; i < n; i++ {
		_ = mustRecv(t, recv, 2*time.Second)
	}
}

func TestWSConn_WriteError_StopsWriterAndClearsQueue(t *testing.T) {
	recvCh := make(chan []byte, 1024)
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Logf("upgrade error: %v", err)
			return
		}
		// 读到第一条后立即关闭，触发 client 侧 write error
		go func() {
			defer func() { _ = conn.Close() }()
			_, data, err := conn.ReadMessage()
			if err == nil {
				// Split packets in first ws message and forward the first packet (if any).
				pos := 0
				if len(data) >= kkpacket.DefaultStreamPacket().LengthFieldByteCount() {
					size, e := kkpacket.DefaultStreamPacket().GetBodySize(data[pos:])
					if e == nil {
						totalLen := kkpacket.DefaultStreamPacket().LengthFieldByteCount() + size
						if len(data) >= totalLen {
							cp := make([]byte, totalLen)
							copy(cp, data[:totalLen])
							select {
							case recvCh <- cp:
							default:
							}
						}
					}
				}
			}
			// close immediately
			_ = conn.Close()
		}()
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"

	c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}

	opts := kknet.ApplyOptions(kknet.WithSendQueueSize(256))
	wc := newWSConn(c, opts, nil)
	defer wc.Close()

	// enqueue multiple messages quickly; some will be pending when peer closes.
	const n = 50
	for i := 0; i < n; i++ {
		bb, err := kkpacket.DefaultStreamPacket().Pack([]byte(fmt.Sprintf("e%d", i)))
		if err != nil {
			t.Fatalf("pack error: %v", err)
		}
		if err := wc.SendBuffer(bb); err != nil {
			t.Fatalf("SendBuffer error: %v", err)
		}
	}

	// Server should get at least the first message (best-effort).
	_ = mustRecv(t, recvCh, 2*time.Second)

	// Writer should exit after write error.
	select {
	case <-wc.writeDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("writer did not stop after write error")
	}

	// Queue should be drained (everything released) after writer stops.
	wc.sendMu.Lock()
	qlen := wc.sendQueue.Len()
	wc.sendMu.Unlock()
	if qlen != 0 {
		t.Fatalf("send queue not drained, Len()=%d", qlen)
	}
}

func TestWSConn_Close_NoFlush_ReturnsQuickly(t *testing.T) {
	wsURL, _, closeSrv := startTestWSServer(t)
	defer closeSrv()

	c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}

	opts := kknet.ApplyOptions(kknet.WithSendQueueSize(256))
	opts.SendQueueNeedFlushOver = false
	wc := newWSConn(c, opts, nil)

	// enqueue some messages, then close immediately. We only assert it doesn't block.
	for i := 0; i < 100; i++ {
		bb, err := kkpacket.DefaultStreamPacket().Pack([]byte(fmt.Sprintf("c%d", i)))
		if err != nil {
			t.Fatalf("pack error: %v", err)
		}
		if err := wc.SendBuffer(bb); err != nil {
			t.Fatalf("SendBuffer error: %v", err)
		}
	}

	done := make(chan struct{})
	go func() {
		_ = wc.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("Close blocked unexpectedly")
	}

	// writer should stop quickly as well.
	select {
	case <-wc.writeDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("writer did not stop after Close")
	}
}

