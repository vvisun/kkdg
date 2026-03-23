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

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkprocessor"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

type clientHandler struct {
}

func (h *clientHandler) OnNoneCopy(connID kknet.CONN_ID, data []byte) {

}

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
	numConns := 222     //连接数
	msgsPerConn := 1222 //每个连接发送的消息数
	payload := make([]byte, 1024)

	totalMsgs := int64(numConns * msgsPerConn)

	addr := freePortStress(t)
	svrHandler := &stressRecvHandler{target: totalMsgs, ch: make(chan struct{})}
	opts := kknet.ApplyOptions(
		kknet.WithRpProvider(kkprocessor.NewReadProcessor),
		kknet.WithRawHandler(svrHandler),
		//kknet.WithNoneCopyHandler(svrHandler),
		kknet.WithWpProvider(kkprocessor.NewWorkerWriteProcessor),
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

	for i := range payload {
		payload[i] = 0x01
	}

	clientOpts := []kknet.Option{
		kknet.WithRpProvider(kkprocessor.NewSyncReadProcessor),
		kknet.WithNoneCopyHandler(&clientHandler{}),
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
				bb, err := testStreamTool.Pack(payload)
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

	timedOut := false
	select {
	case <-svrHandler.ch:
	case <-time.After(10 * time.Second):
		timedOut = true
	}

	clientsMu.Lock()
	for _, c := range clients {
		_ = c.Close()
	}
	clientsMu.Unlock()

	got := svrHandler.Count()
	elapsed := time.Since(start)
	kklog.Debugf("ws server received %d, total: %d, rate: %f", got, totalMsgs, float64(got)/float64(totalMsgs))
	kklog.Debugf("ws stress: send done in %v, all done in %v, recv/s ≈ %.0f",
		sendDone, elapsed, float64(got)/elapsed.Seconds())
	if timedOut {
		t.Fatalf("stress timeout: server received %d/%d", got, totalMsgs)
	}
	if got != totalMsgs {
		t.Fatalf("server received %d, want %d", got, totalMsgs)
	}
}

// TestStress_ServerToSingleClient: 服务器向单个客户端发送大量消息。
func TestStress_ServerToSingleClient(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	runStressServerToClients(t, 1, 1024*4, 8888)
}

// TestStress_ServerToFourClients: 服务器向多客户端发送大量消息（轮询分发）。
func TestStress_ServerToFourClients(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	runStressServerToClients(t, 8, 1024*8, 8888)
}

func runStressServerToClients(t *testing.T, numClients int, batchSize int, totalMsgs int) {
	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = 0x02
	}
	addr := freePortStress(t)
	serverAddr := "ws://" + addr + "/ws"
	clientRecv := &stressRecvHandler{target: int64(totalMsgs), ch: make(chan struct{})}

	opts := kknet.ApplyOptions(
		kknet.WithLogger(kklog.Nop()),
		kknet.WithRawHandler(&noopRawHandler{}),
		kknet.WithNoneCopyHandler(&clientHandler{}),
		kknet.WithRpProvider(kkprocessor.NewSyncReadProcessor),
		kknet.WithWpProvider(kkprocessor.NewWorkerWriteProcessor),
		kknet.WithRecvQueueSize(512),
		kknet.WithBufferSizes(2*1024, 2*1024),
		kknet.WithSendQueueNeedFlushOver(true),
		kknet.WithSendQueueTimeoutFlushOver(5*time.Second),
	)
	srv := NewServer(addr, nil, opts)
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Stop()
	waitTCPReady(t, addr, 2*time.Second)

	clientOpts := kknet.ApplyOptions(
		kknet.WithLogger(kklog.Nop()),
		kknet.WithRpProvider(kkprocessor.NewSyncReadProcessor),
		kknet.WithRawHandler(clientRecv),
		kknet.WithNoneCopyHandler(clientRecv),
		kknet.WithWpProvider(kkprocessor.NewWorkerWriteProcessor),
		kknet.WithBufferSizes(2*1024, 2*1024),
		kknet.WithRecvQueueSize(512),
	)
	clients := make([]*Client, numClients)
	for i := 0; i < numClients; i++ {
		clients[i] = NewClient(serverAddr, nil, clientOpts)
		if err := connectWithRetry(clients[i], 30, 10*time.Millisecond); err != nil {
			t.Fatalf("client[%d] Connect: %v", i, err)
		}
		defer clients[i].Close()
	}

	time.Sleep(100 * time.Millisecond)
	connIDs := make([]kknet.CONN_ID, 0, numClients)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mgr := srv.GetConnManager()
		connIDs = connIDs[:0]
		mgr.RangeAllConns(func(id kknet.CONN_ID, _ kknet.IConn) bool {
			if mgr.GetConn(id) != nil {
				connIDs = append(connIDs, id)
			}
			return true
		})
		if len(connIDs) >= numClients {
			connIDs = connIDs[:numClients]
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(connIDs) < numClients {
		t.Fatalf("timeout: got %d connections, want %d", len(connIDs), numClients)
	}

	start := time.Now()
	for i := 0; i < totalMsgs; i++ {
		bb, err := testStreamTool.Pack(payload)
		if err != nil {
			t.Fatalf("Pack: %v", err)
		}
		connID := connIDs[i%numClients]
		if err := srv.SendBuffer(connID, bb); err != nil {
			t.Fatalf("SendBuffer: %v", err)
		}
		if (i+1)%batchSize == 0 {
			time.Sleep(1 * time.Millisecond)
		}
	}
	sendDone := time.Since(start)

	select {
	case <-clientRecv.ch:
	case <-time.After(15 * time.Second):
		got := clientRecv.Count()
		t.Fatalf("timeout: clients received %d/%d", got, totalMsgs)
	}
	got := clientRecv.Count()
	elapsed := time.Since(start)
	kklog.Debugf("kkws server->%d clients: sent %d, clients received %d, send done in %v, all in %v, recv/s ≈ %.0f",
		numClients, totalMsgs, got, sendDone, elapsed, float64(got)/elapsed.Seconds())
	if got != int64(totalMsgs) {
		t.Errorf("clients received %d, want %d", got, totalMsgs)
	}
}

// TestStress_ManyConns_ConnectDisconnect: rapid connect/disconnect to stress connection lifecycle.
func TestStress_ManyConns_ConnectDisconnect(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	rounds := 100
	connsPerRound := 20

	addr := freePortStress(t)
	opts := kknet.ApplyOptions(
		kknet.WithRawHandler(&noopRawHandler{}),
		kknet.WithLogger(kklog.Nop()),
	)
	srv := NewServer(addr, nil, opts)
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
				client := NewClient("ws://"+addr+"/ws", nil, opts)
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
