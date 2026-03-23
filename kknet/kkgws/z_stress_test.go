// 压测：多连接/高吞吐/快速建连断连。
// 运行：go test -run TestStress -v -timeout=90s ./kknet/kkgws
// 跳过：go test -short 会跳过压测。
package kkgws

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

type clientHandler struct{}

func (h *clientHandler) OnNoneCopy(connID kknet.CONN_ID, data []byte) {}

type stressRecvHandler struct {
	recvCount atomic.Int64
	ch        chan struct{}
	target    int64
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

type noopRawHandler struct{}

func (h *noopRawHandler) OnRaw(_ kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	kkbuffer.Put(data)
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

func connectWithRetry(client *Client, attempts int, baseBackoff time.Duration) error {
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

// TestStress_ManyConns_ManyMessages: 多连接、每连接多发消息。
func TestStress_ManyConns_ManyMessages(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	numConns := 222
	msgsPerConn := 1222
	payload := make([]byte, 1024)

	totalMsgs := int64(numConns * msgsPerConn)

	addr := freePort(t)
	svrHandler := &stressRecvHandler{target: totalMsgs, ch: make(chan struct{})}
	opts := kknet.ApplyOptions(
		kknet.WithRpProvider(kkprocessor.NewReadProcessor),
		kknet.WithRawHandler(svrHandler),
		kknet.WithNoneCopyHandler(svrHandler),
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

	go func() {
		for {
			time.Sleep(1000 * time.Millisecond)
			stats := srv.Stats()
			kknet.PrintStress(&stats)
		}
	}()

	serverAddr := "ws://" + addr + "/ws"
	for i := range payload {
		payload[i] = 0x01
	}

	clientOpts := kknet.ApplyOptions(
		kknet.WithRpProvider(kkprocessor.NewSyncReadProcessor),
		kknet.WithNoneCopyHandler(&clientHandler{}),
		kknet.WithSendQueueNeedFlushOver(true),
		kknet.WithSendQueueTimeoutFlushOver(5*time.Second),
		kknet.WithBufferSizes(2*1024, 2*1024),
		kknet.WithLogger(kklog.Nop()),
	)
	start := time.Now()
	var wg sync.WaitGroup
	errCh := make(chan error, numConns)
	var clientsMu sync.Mutex
	clients := make([]*Client, 0, numConns)
	connSem := make(chan struct{}, 200)
	for i := 0; i < numConns; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			connSem <- struct{}{}
			defer func() { <-connSem }()
			client := NewClient(serverAddr, nil, clientOpts)
			if err := connectWithRetry(client, 30, 10*time.Millisecond); err != nil {
				errCh <- err
				return
			}
			for j := 0; j < msgsPerConn; j++ {
				bb, err := clientOpts.StreamTool.Pack(payload)
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
	case <-svrHandler.ch:
	case <-time.After(10 * time.Second):
		got := svrHandler.Count()
		kklog.Debugf("kkgws stress: timeout %d/%d received, rate: %f", got, totalMsgs, float64(got)/float64(totalMsgs))
	}

	clientsMu.Lock()
	for _, c := range clients {
		_ = c.Close()
	}
	clientsMu.Unlock()

	got := svrHandler.Count()
	elapsed := time.Since(start)
	kklog.Debugf("kkgws server received %d, total: %d, rate: %f", got, totalMsgs, float64(got)/float64(totalMsgs))
	kklog.Debugf("kkgws stress: send done in %v, all done in %v, recv/s ≈ %.0f",
		sendDone, elapsed, float64(got)/elapsed.Seconds())
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
	addr := freePort(t)
	serverAddr := "ws://" + addr + "/ws"
	clientRecv := &stressRecvHandler{target: int64(totalMsgs), ch: make(chan struct{})}

	opts := kknet.ApplyOptions(
		kknet.WithLogger(kklog.Nop()),
		kknet.WithRawHandler(&noopRawHandler{}),
		kknet.WithNoneCopyHandler(&clientHandler{}),
		kknet.WithRpProvider(kkprocessor.NewSyncReadProcessor),
		kknet.WithRecvQueueSize(64),
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
		kknet.WithBufferSizes(2*1024, 2*1024),
		kknet.WithRecvQueueSize(64),
	)
	clients := make([]*Client, numClients)
	for i := 0; i < numClients; i++ {
		clients[i] = NewClient(serverAddr, nil, clientOpts)
		if err := clients[i].Connect(); err != nil {
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
		bb, err := opts.StreamTool.Pack(payload)
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
	kklog.Debugf("kkgws server->%d clients: sent %d, clients received %d, send done in %v, all in %v, recv/s ≈ %.0f",
		numClients, totalMsgs, got, sendDone, elapsed, float64(got)/elapsed.Seconds())
	if got != int64(totalMsgs) {
		t.Errorf("clients received %d, want %d", got, totalMsgs)
	}
}

// TestStress_ManyConns_ConnectDisconnect: 快速建连/断连，压测连接生命周期。
func TestStress_ManyConns_ConnectDisconnect(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	rounds := 100
	connsPerRound := 20

	addr := freePort(t)
	opts := kknet.ApplyOptions(
		kknet.WithRawHandler(&noopRawHandler{}),
		kknet.WithLogger(kklog.Nop()),
	)
	srv := NewServer(addr, nil, opts)
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
	kklog.Debugf("kkgws connect/disconnect: %d rounds × %d conns = %d total in %v, ≈ %.0f conn/s",
		rounds, connsPerRound, totalConns, elapsed, float64(totalConns)/elapsed.Seconds())
}
