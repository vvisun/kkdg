// 压测：多连接/高吞吐/快速建连断连。
// 运行：go test -run TestStress -v -timeout=90s ./kknet/kktcp
// 跳过：go test -short 会跳过压测。
package kktcp

import (
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kkprocessor"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

type clientHandler struct {
}

func (h *clientHandler) OnNoneCopy(connID kknet.CONN_ID, data []byte) {

}

type stressRecvHandler struct {
	recvCount atomic.Int64
	ch        chan struct{}
	target    int64
	closeOnce sync.Once
}

func (h *stressRecvHandler) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	if data == nil {
		return
	}
	n := h.recvCount.Add(1)
	kkbuffer.Put(data)
	if h.target > 0 && h.ch != nil && n >= h.target {
		h.closeOnce.Do(func() { close(h.ch) })
	}
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
	if strings.Contains(strings.ToLower(err.Error()), "refused") {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		if errors.Is(opErr.Err, syscall.ECONNREFUSED) {
			return true
		}
	}
	return false
}

func connectWithRetry(client *GnetClient, attempts int, baseBackoff time.Duration) error {
	if attempts < 1 {
		attempts = 1
	}
	if baseBackoff <= 0 {
		baseBackoff = 10 * time.Millisecond
	}
	var lastErr error
	for i := 0; i < attempts; i++ {
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

// TestStress_ManyConns_ManyMessages: 多连接并发，每连接发送多条消息。
func TestStress_ManyConns_ManyMessages(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	numConns := 5555
	msgsPerConn := 222
	payload := make([]byte, 512)
	totalMsgs := int64(numConns * msgsPerConn)

	addr := freePortStress(t)
	recv := &stressRecvHandler{target: totalMsgs, ch: make(chan struct{})}
	// 高连接数时用较小读写缓冲以降低内存：50k 连接 × (2KB+2KB) ≈ 200MB，默认 64KB×2 约 6.4GB
	opts := kknet.ApplyOptions(
		kknet.WithNoneCopyHandler(recv),
		kknet.WithRpProvider(kkprocessor.NewSyncReadProcessor),
		kknet.WithRecvQueueSize(512),
		kknet.WithBufferSizes(2*1024, 2*1024),
	)
	srv := NewServer(addr, nil, opts)
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Stop()
	waitTCPReady(t, addr, 2*time.Second)

	go func() {
		for {
			time.Sleep(1 * time.Second)
			stats := srv.Stats()
			kknet.PrintStress(&stats)
		}
	}()

	//--------client--------

	for i := range payload {
		payload[i] = 0x01
	}

	clientOpts := []kknet.Option{
		kknet.WithRpProvider(kkprocessor.NewSyncReadProcessor),
		kknet.WithNoneCopyHandler(&clientHandler{}),
		kknet.WithSendQueueNeedFlushOver(true),
		kknet.WithSendQueueTimeoutFlushOver(5 * time.Second),
		kknet.WithBufferSizes(2*1024, 2*1024),
	}
	start := time.Now()
	var wg sync.WaitGroup
	errCh := make(chan error, numConns)
	var clientsMu sync.Mutex
	clients := make([]*GnetClient, 0, numConns)
	connSem := make(chan struct{}, 100)
	for i := 0; i < numConns; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			connSem <- struct{}{}
			defer func() { <-connSem }()
			client := NewClient(addr, nil, kknet.ApplyOptions(clientOpts...))
			if err := connectWithRetry(client, 50, 20*time.Millisecond); err != nil {
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
	kklog.Debugf("tcp server received %d, total: %d, rate: %f", got, totalMsgs, float64(got)/float64(totalMsgs))
	kklog.Debugf("tcp stress: send done in %v, all done in %v, recv/s ≈ %.0f",
		sendDone, elapsed, float64(got)/elapsed.Seconds())
}

// TestStress_ManyConns_ConnectDisconnect: 快速建连/断连，压测连接生命周期。
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
	waitTCPReady(t, addr, 2*time.Second)

	start := time.Now()
	for r := 0; r < rounds; r++ {
		var wg sync.WaitGroup
		for i := 0; i < connsPerRound; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				client := NewClient(addr, nil, kknet.DefaultOptions())
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
