// 压测：多连接/高吞吐/快速建连断连（TLS 版）。
// 运行：go test -run TestStress -v -timeout=90s ./kknet/kktcptls
// 跳过：go test -short 会跳过压测。
package kktcptls

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

//---------------- common helpers (copied/adapted from kktcp z_stress_test) ----------------

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

// clientHandler is a noop NoneCopyHandler used on client side during stress tests.
type clientHandler struct{}

func (h *clientHandler) OnNoneCopy(connID kknet.CONN_ID, data []byte) {}

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

// isConnTimeout 判断是否为建连超时（可重试，如 accept 忙或 backlog 满）。
func isConnTimeout(err error) bool {
	if err == nil {
		return false
	}
	if strings.Contains(strings.ToLower(err.Error()), "timeout") ||
		strings.Contains(strings.ToLower(err.Error()), "did not properly respond") {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return false
}

// TLS 客户端重连封装；与 kktcp 的 connectWithRetry 类似，只是客户端类型不同。
func connectWithRetryTLS(client *Client, attempts int, baseBackoff time.Duration) error {
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
			// 仅对“可重试”的错误重试：连接被拒绝（服务未就绪）或建连超时（高并发下 accept 慢）
			if !isConnRefused(err) && !isConnTimeout(err) {
				return err
			}
		}
		time.Sleep(baseBackoff + time.Duration(i)*baseBackoff)
	}
	return lastErr
}

//---------------- stress tests ----------------

// TestStress_ManyConns_ManyMessages_TLS: 多连接并发，每连接发送多条消息（TLS）。
func TestStress_ManyConns_ManyMessages_TLS(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	numConns := 16
	msgsPerConn := 8888
	payload := make([]byte, 512)
	totalMsgs := int64(numConns * msgsPerConn)

	addr := freePortStress(t)
	tlsCfg := genTestTLSConfig(t)

	recv := &stressRecvHandler{target: totalMsgs, ch: make(chan struct{})}
	// 高连接数时用较小读写缓冲以降低内存
	srvOpts := kknet.ApplyOptions(
		kknet.WithLogger(kklog.Nop()),
		kknet.WithRawHandler(recv),
		kknet.WithRpProvider(kkprocessor.NewWorkerReadProcessor),
		kknet.WithWpProvider(kkprocessor.NewWorkerWriteProcessor),
		kknet.WithRecvQueueSize(64),
		kknet.WithWorkerQueueMaxConcurrency(1),
		kknet.WithBufferSizes(2*1024, 2*1024),
		kknet.WithTLSConfig(tlsCfg),
	)
	srv := NewServer(addr, nil, srvOpts)
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

	//-------- TLS clients --------

	for i := range payload {
		payload[i] = 0x01
	}

	start := time.Now()
	var wg sync.WaitGroup
	errCh := make(chan error, numConns)
	var clientsMu sync.Mutex
	clients := make([]*Client, 0, numConns)

	// 限制并发建连+发送，避免服务器侧过载
	connSem := make(chan struct{}, 50)
	for i := 0; i < numConns; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			connSem <- struct{}{}
			defer func() { <-connSem }()

			// 为客户端克隆一份 TLS 配置
			clientCfg := tlsCfg.Clone()
			if clientCfg == nil {
				// 保守处理，避免 nil panic
				tmp := *tlsCfg
				clientCfg = &tmp
			}
			clientCfg.InsecureSkipVerify = true

			cliOpts := kknet.ApplyOptions(
				kknet.WithRpProvider(kkprocessor.NewSyncReadProcessor),
				kknet.WithNoneCopyHandler(&clientHandler{}),
				kknet.WithSendQueueNeedFlushOver(true),
				kknet.WithSendQueueTimeoutFlushOver(5*time.Second),
				kknet.WithBufferSizes(2*1024, 2*1024),
				kknet.WithRecvQueueSize(64),
				kknet.WithTLSConfig(clientCfg),
				kknet.WithIsNeedReconnect(false),
			)
			client := NewClient(addr, nil, cliOpts)
			if err := connectWithRetryTLS(client, 80, 30*time.Millisecond); err != nil {
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
				if (j+1)%128 == 0 {
					time.Sleep(1 * time.Millisecond)
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
			t.Fatalf("TLS stress send error: %v", err)
		}
	}

	select {
	case <-recv.ch:
	case <-time.After(15 * time.Second):
		got := recv.Count()
		kklog.Debugf("kktcptls stress: timeout %d/%d received, rate: %f", got, totalMsgs, float64(got)/float64(totalMsgs))
	}

	clientsMu.Lock()
	for _, c := range clients {
		_ = c.Close()
	}
	clientsMu.Unlock()

	got := recv.Count()
	elapsed := time.Since(start)
	kklog.Debugf("kktcptls server received %d, total: %d, rate: %f", got, totalMsgs, float64(got)/float64(totalMsgs))
	kklog.Debugf("kktcptls stress: send done in %v, all done in %v, recv/s ≈ %.0f",
		sendDone, elapsed, float64(got)/elapsed.Seconds())
}

// TestStress_ManyConns_ConnectDisconnect_TLS: 快速建连/断连，压测 TLS 连接生命周期。
func TestStress_ManyConns_ConnectDisconnect_TLS(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	rounds := 50
	connsPerRound := 20

	addr := freePortStress(t)
	tlsCfg := genTestTLSConfig(t)
	srvOpts := kknet.ApplyOptions(
		kknet.WithLogger(kklog.Nop()),
		kknet.WithTLSConfig(tlsCfg),
		kknet.WithRpProvider(kkprocessor.NewSyncReadProcessor),
		kknet.WithNoneCopyHandler(&clientHandler{}),
	)
	srv := NewServer(addr, nil, srvOpts)
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
				clientCfg := tlsCfg.Clone()
				if clientCfg == nil {
					tmp := *tlsCfg
					clientCfg = &tmp
				}
				clientCfg.InsecureSkipVerify = true
				cliOpts := kknet.ApplyOptions(
					kknet.WithTLSConfig(clientCfg),
					kknet.WithIsNeedReconnect(false),
					kknet.WithRpProvider(kkprocessor.NewSyncReadProcessor),
					kknet.WithNoneCopyHandler(&clientHandler{}),
					kknet.WithIsNeedReconnect(false),
				)
				client := NewClient(addr, nil, cliOpts)
				_ = client.Connect()
				time.Sleep(5 * time.Millisecond)
				_ = client.Close()
			}()
		}
		wg.Wait()
	}
	elapsed := time.Since(start)
	totalConns := rounds * connsPerRound
	kklog.Debugf("kktcptls connect/disconnect: %d rounds × %d conns = %d total in %v, ≈ %.0f conn/s",
		rounds, connsPerRound, totalConns, elapsed, float64(totalConns)/elapsed.Seconds())
}
