// 压测：多连接/高吞吐/快速建连断连。
// 运行：go test -run TestStress -v -timeout=90s
// 跳过：go test -short 会跳过压测。
package kkws

import (
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kkprocessor"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

// stressRecvHandler counts received messages for stress tests.
type stressRecvHandler struct {
	recvCount atomic.Int64
	ch        chan struct{} // optional: closed when target count reached
	target    int64         // 0 = no target
	closeOnce sync.Once
}

func (h *stressRecvHandler) OnNoneCopy(connID kknet.CONN_ID, data []byte) {
	if data == nil {
		return
	}
	n := h.recvCount.Add(1)
	if h.target > 0 && h.ch != nil && n >= h.target {
		h.closeOnce.Do(func() { close(h.ch) })
	}
}

func (h *stressRecvHandler) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	if data == nil {
		return
	}
	n := h.recvCount.Add(1)
	if h.target > 0 && h.ch != nil && n >= h.target {
		h.closeOnce.Do(func() { close(h.ch) })
	}
	kkbuffer.Put(data)
}

func (h *stressRecvHandler) Count() int64 { return h.recvCount.Load() }

func freePortStress(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func waitTCPReady(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("server not ready on %s after %v", addr, timeout)
}

func isConnRefused(err error) bool {
	if err == nil {
		return false
	}
	// Windows typically surfaces "connectex: ... actively refused ..."
	if strings.Contains(strings.ToLower(err.Error()), "refused") {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		// best-effort: match common errno
		if errors.Is(opErr.Err, syscall.ECONNREFUSED) {
			return true
		}
	}
	return false
}

func connectWithRetry(client *Client, attempts int, baseBackoff time.Duration) error {
	if attempts < 1 {
		attempts = 1
	}
	if baseBackoff <= 0 {
		baseBackoff = 10 * time.Millisecond
	}
	var lastErr error
	for i := 0; i < attempts; i++ {
		// time.Sleep(time.Duration(xrand.Int64(5, 50)) * time.Millisecond)
		if err := client.Connect(); err == nil {
			return nil
		} else {
			lastErr = err
			if !isConnRefused(err) {
				return err
			}
		}
		time.Sleep(baseBackoff + time.Duration(i)*baseBackoff)
	}
	return lastErr
}

// TestStress_ManyConns_ManyMessages: many concurrent connections, each sending many messages.
func TestStress_ManyConns_ManyMessages(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	numConns := 12222  //连接数
	msgsPerConn := 555 //每个连接发送的消息数
	totalMsgs := int64(numConns * msgsPerConn)

	addr := freePortStress(t)
	recv := &stressRecvHandler{target: totalMsgs, ch: make(chan struct{})}
	opts := kknet.ApplyOptions(
		kknet.WithRpProvider(kkprocessor.NewSyncReadProcessor),
		//kknet.WithRawHandler(recv),
		kknet.WithNoneCopyHandler(recv),
		kknet.WithRecvQueueSize(512),
		kknet.WithLogger(kklog.Nop()),
		kknet.WithBufferSizes(2*1024, 2*1024),
	)
	srv := NewServer(addr, nil, opts)
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Stop()
	waitTCPReady(t, addr, 2*time.Second)

	//定时打印服务器统计信息
	go func() {
		for {
			time.Sleep(1000 * time.Millisecond)
			stats := srv.Stats()
			kknet.PrintStress(&stats)
		}
	}()

	//--------------------------------------------

	serverAddr := "ws://" + addr + "/ws"

	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = 0x01
	}

	clientOpts := []kknet.Option{
		kknet.WithSendQueueNeedFlushOver(true),
		kknet.WithSendQueueTimeoutFlushOver(5 * time.Second),
		kknet.WithBufferSizes(2*1024, 2*1024),
		kknet.WithLogger(kklog.Nop()),
	}
	start := time.Now()
	var wg sync.WaitGroup
	errCh := make(chan error, numConns)
	var clientsMu sync.Mutex
	clients := make([]*Client, 0, numConns)
	// Ramp up connections to avoid overwhelming accept backlog on Windows.
	connSem := make(chan struct{}, 200)
	for i := 0; i < numConns; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			connSem <- struct{}{}
			defer func() { <-connSem }()
			client := NewClient(serverAddr, nil, kknet.ApplyOptions(clientOpts...))
			if err := connectWithRetry(client, 30, 10*time.Millisecond); err != nil {
				errCh <- err
				return
			}
			for j := 0; j < msgsPerConn; j++ {
				bb, err := kkpacket.DefaultStreamPacket().Pack(payload)
				if err != nil {
					errCh <- err
					return
				}
				if err := client.SendBuffer(bb); err != nil {
					errCh <- err
					return
				}
			}
			clientsMu.Lock()
			clients = append(clients, client)
			clientsMu.Unlock()
		}()
	}
	wg.Wait()

	sendDone := time.Since(start)
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("stress send error: %v", err)
		}
	}

	select {
	case <-recv.ch:
	case <-time.After(10 * time.Second):
		// under load a few messages may still be in flight
		got := recv.Count()
		kklog.Debugf("stress: timeout %d/%d received, rate: %f", got, totalMsgs, float64(got)/float64(totalMsgs))
	}

	clientsMu.Lock()
	for _, c := range clients {
		_ = c.Close()
	}
	clientsMu.Unlock()

	got := recv.Count()
	elapsed := time.Since(start)
	kklog.Debugf("ws server received %d, total: %d, rate: %f", got, totalMsgs, float64(got)/float64(totalMsgs))
	kklog.Debugf("ws stress: send done in %v, all done in %v, recv/s ≈ %.0f",
		sendDone, elapsed, float64(got)/elapsed.Seconds())
}

// TestStress_ManyConns_ConnectDisconnect: rapid connect/disconnect to stress connection lifecycle.
func TestStress_ManyConns_ConnectDisconnect(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	rounds := 100
	connsPerRound := 20

	addr := freePortStress(t)
	srv := NewServer(addr, nil, kknet.DefaultOptions())
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Stop()

	start := time.Now()
	for r := 0; r < rounds; r++ {
		var wg sync.WaitGroup
		for i := 0; i < connsPerRound; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				client := NewClient("ws://"+addr+"/ws", nil, kknet.DefaultOptions())
				_ = client.Connect()
				time.Sleep(5 * time.Millisecond)
				_ = client.Close()
			}()
		}
		wg.Wait()
	}
	elapsed := time.Since(start)
	totalConns := rounds * connsPerRound
	kklog.Debugf("connect/disconnect: %d rounds × %d conns = %d total in %v, ≈ %.0f conn/s",
		rounds, connsPerRound, totalConns, elapsed, float64(totalConns)/elapsed.Seconds())
}

func runClients(addr string, connNum int, msgSize int, sendInterval time.Duration) {
	u := "ws://" + addr + "/ws"

	var wg sync.WaitGroup
	wg.Add(connNum)

	// 构造固定大小的测试消息（二进制）
	msg := make([]byte, msgSize)
	for i := range msg {
		msg[i] = 0x01 // 填充固定内容
	}
	bb, err := kkpacket.DefaultStreamPacket().Pack(msg)
	if err != nil {
		println("pack error:", err.Error())
		return
	}
	msg = bb.Bytes()

	// 启动N个协程，每个协程对应1个WS连接
	for i := 0; i < connNum; i++ {
		go func() {
			defer wg.Done()
			// 建立WS连接
			conn, _, err := websocket.DefaultDialer.Dial(u, nil)
			if err != nil {
				println("dial error:", err.Error())
				return
			}
			defer conn.Close()

			// 后台读消息（Echo返回的消息，防止读缓冲区满）
			go func() {
				for {
					_, _, err := conn.ReadMessage()
					if err != nil {
						return
					}
				}
			}()

			// 定时发消息，模拟业务场景
			ticker := time.NewTicker(sendInterval)
			defer ticker.Stop()
			for range ticker.C {
				err := conn.WriteMessage(websocket.BinaryMessage, msg)
				if err != nil {
					return
				}
			}
		}()
		// 连接建立间隔1ms，避免瞬间压垮服务端
		time.Sleep(1 * time.Millisecond)
	}

	println("=== 压测启动 ===")
	println("目标地址：", addr)
	println("并发连接：", connNum)
	println("消息大小：", msgSize, "B")
	println("发送间隔：", sendInterval)
	println("================\n")
	wg.Wait()
}
