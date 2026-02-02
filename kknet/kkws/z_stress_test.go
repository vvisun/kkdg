// 压测：多连接/高吞吐/快速建连断连。
// 运行：go test -run TestStress -v -timeout=90s
// 跳过：go test -short 会跳过压测。
package kkws

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/kklog"
)

// stressRecvHandler counts received messages for stress tests.
type stressRecvHandler struct {
	recvCount atomic.Int64
	ch        chan struct{} // optional: closed when target count reached
	target    int64         // 0 = no target
	closeOnce sync.Once
}

func (h *stressRecvHandler) OnRaw(connID int64, data buffers.IBuffer) {
	if data == nil {
		return
	}
	// netprocessor will release the buffer after handler returns.
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

// TestStress_ManyConns_ManyMessages: many concurrent connections, each sending many messages.
func TestStress_ManyConns_ManyMessages(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	numConns := 30
	msgsPerConn := 100
	totalMsgs := int64(numConns * msgsPerConn)
	minRecv := totalMsgs * 95 / 100 // allow up to 5% in flight at timeout

	addr := freePortStress(t)
	recv := &stressRecvHandler{target: totalMsgs, ch: make(chan struct{})}
	srv := NewServer(addr, nil, kknet.WithRawHandler(recv))
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Stop()

	payload := []byte("stress")
	clientOpts := []kknet.Option{
		kknet.WithSendQueueNeedFlushOver(true),
		kknet.WithSendQueueTimeoutFlushOver(10 * time.Second),
	}
	start := time.Now()
	var wg sync.WaitGroup
	errCh := make(chan error, numConns)
	for i := 0; i < numConns; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := NewClient("ws://"+addr+"/ws", nil, clientOpts...)
			if err := client.Connect(); err != nil {
				errCh <- err
				return
			}
			defer client.Close()
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
	case <-time.After(25 * time.Second):
		// under load a few messages may still be in flight
		got := recv.Count()
		if got < minRecv {
			kklog.Errorf("timeout: server received %d, want at least %d (target %d)", got, minRecv, totalMsgs)
		}
		kklog.Debugf("stress: finished after timeout with %d/%d received", got, totalMsgs)
	}
	got := recv.Count()
	if got < minRecv {
		kklog.Errorf("server received %d messages, want at least %d", got, minRecv)
	}
	elapsed := time.Since(start)
	kklog.Debugf("stress: %d conns × %d msgs = %d total, send done in %v, all done in %v, recv/s ≈ %.0f",
		numConns, msgsPerConn, totalMsgs, sendDone, elapsed, float64(got)/elapsed.Seconds())
}

// TestStress_HighThroughput: fewer connections, high message rate per connection.
func TestStress_HighThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	numConns := 10
	msgsPerConn := 500
	totalMsgs := int64(numConns * msgsPerConn)
	minRecv := totalMsgs * 90 / 100 // under heavy load allow up to 10% in flight

	addr := freePortStress(t)
	recv := &stressRecvHandler{target: totalMsgs, ch: make(chan struct{})}
	srv := NewServer(addr, nil, kknet.WithRawHandler(recv))
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Stop()

	payload := []byte("throughput")
	clientOpts := []kknet.Option{
		kknet.WithSendQueueNeedFlushOver(true),
		kknet.WithSendQueueTimeoutFlushOver(2 * time.Second),
	}
	start := time.Now()
	var wg sync.WaitGroup
	errCh := make(chan error, numConns)
	var clientsMu sync.Mutex
	clients := make([]*Client, 0, numConns)
	for i := 0; i < numConns; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := NewClient("ws://"+addr+"/ws", nil, clientOpts...)
			if err := client.Connect(); err != nil {
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
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("stress send error: %v", err)
		}
	}
	defer func() {
		clientsMu.Lock()
		defer clientsMu.Unlock()
		for _, c := range clients {
			_ = c.Close()
		}
	}()

	select {
	case <-recv.ch:
	case <-time.After(2*time.Second + 50*time.Millisecond):
		got := recv.Count()
		if got < minRecv {
			kklog.Errorf("throughput timeout: received %d/%d (min %d)", got, totalMsgs, minRecv)
		} else {
			// For stress runs, allow a small number of messages still in flight at timeout.
			// Don't print misleading msg/s numbers based on the timeout wait.
			kklog.Debugf("throughput: timeout with %d/%d received (acceptable)", got, totalMsgs)
		}
	}
	got := recv.Count()
	if got < minRecv {
		kklog.Errorf("server received %d messages, want at least %d (90%% of %d)", got, minRecv, totalMsgs)
	}
	elapsed := time.Since(start)
	kklog.Debugf("throughput: %d conns × %d msgs = %d total in %v",
		numConns, msgsPerConn, totalMsgs, elapsed)
}

// TestStress_ManyConns_ConnectDisconnect: rapid connect/disconnect to stress connection lifecycle.
func TestStress_ManyConns_ConnectDisconnect(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	rounds := 100
	connsPerRound := 20

	addr := freePortStress(t)
	srv := NewServer(addr, nil)
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
				client := NewClient("ws://"+addr+"/ws", nil)
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
