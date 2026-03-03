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
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
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
					size, err := kkpacket.DefaultStreamPacket().ReadMessageSize(data[pos:])
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
	wc := newWSConn(c, &opts, nil)
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

// fillQueueAndBlockWriteBatch 填充队列并让 writeLoop 阻塞在 writeBatch。
// 发送 2 条让 writeLoop PopMany 并阻塞；再发 2 条填满队列(size=2)。
// 调用方需在测试前 Lock writeMu，测试后 Unlock。
func fillQueueAndBlockWriteBatch(t *testing.T, wc *wsConn, prefix string) {
	t.Helper()
	for i := 0; i < 2; i++ {
		bb, err := kkpacket.DefaultStreamPacket().Pack([]byte(fmt.Sprintf("%s%d", prefix, i)))
		if err != nil {
			t.Fatalf("pack error: %v", err)
		}
		if err := wc.SendBuffer(bb); err != nil {
			t.Fatalf("SendBuffer #%d: %v", i, err)
		}
	}
	time.Sleep(50 * time.Millisecond)
	for i := 2; i < 4; i++ {
		bb, err := kkpacket.DefaultStreamPacket().Pack([]byte(fmt.Sprintf("%s%d", prefix, i)))
		if err != nil {
			t.Fatalf("pack error: %v", err)
		}
		if err := wc.SendBuffer(bb); err != nil {
			t.Fatalf("SendBuffer #%d: %v", i, err)
		}
	}
}

func TestWSConn_SendQueueFullAction_Drop(t *testing.T) {
	wsURL, recv, closeSrv := startTestWSServer(t)
	defer closeSrv()
	c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	opts := kknet.ApplyOptions(kknet.WithSendQueueSize(2))
	opts.WpOptions.SendQueueStrict = true
	opts.WpOptions.SendQueueFullAction = kknet.EWpQueueFullActionDrop
	opts.WpOptions.SendQueueNeedFlushOver = false
	wc := newWSConn(c, &opts, nil)
	defer wc.Close()

	wc.writeMu.Lock()
	fillQueueAndBlockWriteBatch(t, wc, "d")
	bb, _ := kkpacket.DefaultStreamPacket().Pack([]byte("overflow"))
	// Drop 模式：队列满时返回 nil，消息被丢弃
	if err := wc.SendBuffer(bb); err != nil {
		t.Fatalf("Drop mode should return nil when full, got %v", err)
	}
	wc.writeMu.Unlock()

	for i := 0; i < 4; i++ {
		_ = mustRecv(t, recv, 500*time.Millisecond)
	}
	select {
	case b := <-recv:
		t.Fatalf("unexpected extra (overflow should be dropped): %v", b)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestWSConn_SendQueueFullAction_Block(t *testing.T) {
	wsURL, recv, closeSrv := startTestWSServer(t)
	defer closeSrv()
	c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	opts := kknet.ApplyOptions(kknet.WithSendQueueSize(2))
	opts.WpOptions.SendQueueStrict = true
	opts.WpOptions.SendQueueFullAction = kknet.EWpQueueFullActionBlock
	opts.WpOptions.SendQueueNeedFlushOver = false
	wc := newWSConn(c, &opts, nil)

	wc.writeMu.Lock()
	fillQueueAndBlockWriteBatch(t, wc, "b")
	blockErrCh := make(chan error, 1)
	go func() {
		bb, _ := kkpacket.DefaultStreamPacket().Pack([]byte("blocked"))
		blockErrCh <- wc.SendBuffer(bb)
	}()
	// Block 模式：应阻塞
	select {
	case err := <-blockErrCh:
		t.Fatalf("Block mode should block when full, got %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	wc.writeMu.Unlock()

	// Stop 应唤醒阻塞的 SendBuffer，返回 ErrConnectionClosed
	_ = wc.Close()
	select {
	case err := <-blockErrCh:
		if err != kkerrors.ErrConnectionClosed {
			t.Errorf("blocked SendBuffer after Close: got %v, want ErrConnectionClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked SendBuffer should unblock after Close")
	}

	// 应收到 4 条
	for i := 0; i < 4; i++ {
		_ = mustRecv(t, recv, 500*time.Millisecond)
	}
}

func TestWSConn_SendQueueFullAction_Retry(t *testing.T) {
	wsURL, recv, closeSrv := startTestWSServer(t)
	defer closeSrv()
	c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	opts := kknet.ApplyOptions(kknet.WithSendQueueSize(2))
	opts.WpOptions.SendQueueStrict = true
	opts.WpOptions.SendQueueFullAction = kknet.EWpQueueFullActionRetry
	opts.WpOptions.SendQueueRetryMaxCount = 3
	opts.WpOptions.SendQueueRetryInterval = 2 * time.Millisecond
	opts.WpOptions.SendQueueNeedFlushOver = false
	wc := newWSConn(c, &opts, nil)
	defer wc.Close()

	wc.writeMu.Lock()
	fillQueueAndBlockWriteBatch(t, wc, "r")
	bb, _ := kkpacket.DefaultStreamPacket().Pack([]byte("overflow"))
	if err := wc.SendBuffer(bb); err == nil {
		t.Fatal("Retry mode: expected ErrSendQueueFull, got nil")
	} else if err != kkerrors.ErrSendQueueFull {
		t.Fatalf("Retry mode: got %v, want ErrSendQueueFull", err)
	}
	wc.writeMu.Unlock()

	for i := 0; i < 4; i++ {
		_ = mustRecv(t, recv, 500*time.Millisecond)
	}
	select {
	case b := <-recv:
		t.Fatalf("unexpected extra (overflow rejected): %v", b)
	case <-time.After(100 * time.Millisecond):
	}
}

// TestWSConn_SendQueueFullAction_Retry_MaxCount1 边界：RetryMaxCount=1 时第一次失败即返回
func TestWSConn_SendQueueFullAction_Retry_MaxCount1(t *testing.T) {
	wsURL, recv, closeSrv := startTestWSServer(t)
	defer closeSrv()
	c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	opts := kknet.ApplyOptions(kknet.WithSendQueueSize(2))
	opts.WpOptions.SendQueueStrict = true
	opts.WpOptions.SendQueueFullAction = kknet.EWpQueueFullActionRetry
	opts.WpOptions.SendQueueRetryMaxCount = 1
	opts.WpOptions.SendQueueRetryInterval = 1 * time.Millisecond
	opts.WpOptions.SendQueueNeedFlushOver = false
	wc := newWSConn(c, &opts, nil)
	defer wc.Close()

	wc.writeMu.Lock()
	fillQueueAndBlockWriteBatch(t, wc, "m")
	bb, _ := kkpacket.DefaultStreamPacket().Pack([]byte("overflow"))
	if err := wc.SendBuffer(bb); err != kkerrors.ErrSendQueueFull {
		t.Errorf("RetryMaxCount=1: got %v, want ErrSendQueueFull", err)
	}
	wc.writeMu.Unlock()

	for i := 0; i < 4; i++ {
		_ = mustRecv(t, recv, 500*time.Millisecond)
	}
}

// TestWSConn_SendQueueStrict_False 非严格模式：队列满时自动扩容，不应返回 ErrSendQueueFull
func TestWSConn_SendQueueStrict_False(t *testing.T) {
	wsURL, recv, closeSrv := startTestWSServer(t)
	defer closeSrv()
	c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	opts := kknet.ApplyOptions(kknet.WithSendQueueSize(4))
	opts.WpOptions.SendQueueStrict = false
	opts.WpOptions.SendQueueFullAction = kknet.EWpQueueFullActionRetry
	opts.WpOptions.SendQueueNeedFlushOver = false
	wc := newWSConn(c, &opts, nil)
	defer wc.Close()

	// 快速发送较多消息，非严格模式应全部成功
	const n = 100
	for i := 0; i < n; i++ {
		bb, err := kkpacket.DefaultStreamPacket().Pack([]byte(fmt.Sprintf("n%d", i)))
		if err != nil {
			t.Fatalf("pack: %v", err)
		}
		if err := wc.SendBuffer(bb); err != nil {
			t.Fatalf("SendBuffer #%d: %v (strict=false should not fail)", i, err)
		}
	}
	for i := 0; i < n; i++ {
		_ = mustRecv(t, recv, 2*time.Second)
	}
	select {
	case b := <-recv:
		t.Fatalf("unexpected extra: %v", b)
	case <-time.After(100 * time.Millisecond):
	}
}

// TestWSConn_SendQueueFullAction_AfterClose 边界：连接关闭后各模式应返回 ErrConnectionClosed
func TestWSConn_SendQueueFullAction_AfterClose(t *testing.T) {
	actions := []kknet.EWpQueueFullAction{
		kknet.EWpQueueFullActionDrop,
		kknet.EWpQueueFullActionBlock,
		kknet.EWpQueueFullActionRetry,
	}
	names := map[kknet.EWpQueueFullAction]string{
		kknet.EWpQueueFullActionDrop:  "Drop",
		kknet.EWpQueueFullActionBlock: "Block",
		kknet.EWpQueueFullActionRetry: "Retry",
	}
	for _, action := range actions {
		action := action
		t.Run(names[action], func(t *testing.T) {
			wsURL, _, closeSrv := startTestWSServer(t)
			defer closeSrv()
			c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			opts := kknet.ApplyOptions(kknet.WithSendQueueSize(2))
			opts.WpOptions.SendQueueStrict = true
			opts.WpOptions.SendQueueFullAction = action
			opts.WpOptions.SendQueueRetryMaxCount = 3
			wc := newWSConn(c, &opts, nil)
			_ = wc.Close()

			bb, _ := kkpacket.DefaultStreamPacket().Pack([]byte("after-close"))
			err = wc.SendBuffer(bb)
			if err != kkerrors.ErrConnectionClosed {
				t.Errorf("SendBuffer after Close: got %v, want ErrConnectionClosed", err)
			}
		})
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
	opts.WpOptions.SendQueueNeedFlushOver = true
	opts.WpOptions.SendQueueTimeoutFlushOver = 2 * time.Second
	wc := newWSConn(c, &opts, nil)

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
					size, e := kkpacket.DefaultStreamPacket().ReadMessageSize(data[pos:])
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
			// Send close control then close immediately to make client writes fail faster.
			_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(250*time.Millisecond))
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

	opts := kknet.ApplyOptions(
		kknet.WithSendQueueSize(256),
		kknet.WithWriteTimeout(300*time.Millisecond),
		kknet.WithRawHandler(&noopRawHandler{}),
	)
	wc := newWSConn(c, &opts, nil)
	defer wc.Close()

	// In real usage, a read loop will notice peer close and trigger closeWithError,
	// which will stop the writer and release pending buffers.
	go func() {
		err := wc.readLoop()
		wc.closeWithError(nil, err)
	}()

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

	// Writer should exit after peer close is detected.
	select {
	case <-wc.wp.Done():
	case <-time.After(5 * time.Second):
		t.Fatalf("writer did not stop after write error")
	}

	// Queue should be drained (everything released) after writer stops.
	qlen := 0
	if wc.wp != nil {
		qlen = wc.wp.Pending()
	}
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
	opts.WpOptions.SendQueueNeedFlushOver = false
	wc := newWSConn(c, &opts, nil)

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
	case <-wc.wp.Done():
	case <-time.After(2 * time.Second):
		t.Fatalf("writer did not stop after Close")
	}
}

func TestWSConn_RemoteAddr(t *testing.T) {
	wsURL, _, closeSrv := startTestWSServer(t)
	defer closeSrv()

	c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}
	opts := kknet.ApplyOptions()
	wc := newWSConn(c, &opts, nil)
	defer wc.Close()

	addr := wc.RemoteAddr()
	if addr == "" {
		t.Error("RemoteAddr() is empty")
	}
}

func TestWSConn_SendBuffer_AfterClose(t *testing.T) {
	wsURL, _, closeSrv := startTestWSServer(t)
	defer closeSrv()

	c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}
	opts := kknet.ApplyOptions()
	wc := newWSConn(c, &opts, nil)
	_ = wc.Close()

	bb, err := kkpacket.DefaultStreamPacket().Pack([]byte("after close"))
	if err != nil {
		t.Fatalf("pack error: %v", err)
	}
	err = wc.SendBuffer(bb)
	if err != kkerrors.ErrConnectionClosed {
		t.Errorf("SendBuffer after Close = %v, want ErrConnectionClosed", err)
	}
}

func TestWSConn_InvalidPacket(t *testing.T) {
	wsURL, _, closeSrv := startTestWSServer(t)
	defer closeSrv()

	c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}
	opts := kknet.ApplyOptions()
	wc := newWSConn(c, &opts, nil)
	defer wc.Close()

	// buffer too short to be valid packet (length field is 4 bytes)
	invalidBuf := kkbuffer.GetWithCapacity(2)
	invalidBuf.B = invalidBuf.B[:2]
	err = wc.SendBuffer(invalidBuf)
	if err == nil {
		t.Errorf("SendBuffer invalid packet = %v, want error", err)
	}
}
