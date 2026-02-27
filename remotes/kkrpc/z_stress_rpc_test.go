// 压测：多连接/高吞吐 RPC 调用、快速建连断连。
// 运行：go test -run TestStress -v -timeout=90s
// 跳过：go test -short 会跳过压测。
package kkrpc

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/kklog"
)

func freePortRpcStress(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func waitTCPReadyRpc(t *testing.T, addr string, timeout time.Duration) {
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

func isConnRefusedRpc(err error) bool {
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

func startClientWithRetry(client *Client, attempts int, baseBackoff time.Duration) error {
	if attempts < 1 {
		attempts = 1
	}
	if baseBackoff <= 0 {
		baseBackoff = 10 * time.Millisecond
	}
	var lastErr error
	for i := 0; i < attempts; i++ {
		if err := client.Start(); err == nil {
			return nil
		} else {
			lastErr = err
			if !isConnRefusedRpc(err) {
				return err
			}
		}
		time.Sleep(baseBackoff + time.Duration(i)*baseBackoff)
	}
	return lastErr
}

// TestStress_Rpc_ManyConns_ManyCalls 多连接、每连接多次 ReqRsp 调用
func TestStress_Rpc_ManyConns_ManyCalls(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	numConns := 64
	callsPerConn := 8888
	totalCalls := int64(numConns * callsPerConn)

	addr := freePortRpcStress(t)
	ClearRpcManagerForTest()
	RegisterReqRspMethod[testReq, testRsp]("testReqRsp")

	successCount := atomic.Int64{}
	rpcRouter := NewRpcReceiver()
	RegistReqRspHandler(rpcRouter, "testReqRsp", func(ctx context.Context, msg *testReq, resp *testRsp) error {
		resp.Code = 0
		resp.Msg = "ok"
		return nil
	})

	opts := kknet.ApplyOptions(
		kknet.WithLogger(kklog.Nop()),
		kknet.WithBufferSizes(4*1024, 4*1024),
	)
	svr := NewServer(addr, opts, rpcRouter)
	if err := svr.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svr.Stop()
	waitTCPReadyRpc(t, addr, 2*time.Second)

	go func() {
		for {
			time.Sleep(1000 * time.Millisecond)
			n := successCount.Load()
			kklog.Debugf("kkrpc stress: %d calls succeeded", n)
		}
	}()

	start := time.Now()
	var wg sync.WaitGroup
	errCh := make(chan error, numConns)
	clients := make([]*Client, 0, numConns)
	var clientsMu sync.Mutex
	connSem := make(chan struct{}, 100)

	for i := 0; i < numConns; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			connSem <- struct{}{}
			defer func() { <-connSem }()
			cli := NewClient(addr, opts, rpcRouter)
			if err := startClientWithRetry(cli, 30, 10*time.Millisecond); err != nil {
				errCh <- err
				return
			}
			clientsMu.Lock()
			clients = append(clients, cli)
			clientsMu.Unlock()

			invoker := NewReqRspInvoker[testReq, testRsp](cli, 0, "testReqRsp")
			for j := 0; j < callsPerConn; j++ {
				req := testReq{ID: idx*1000 + j, Data: "stress"}
				var resp testRsp
				if err := invoker.Invoke(context.Background(), &req, CallConfig{Timeout: 5 * time.Second}, &resp); err != nil {
					errCh <- err
					return
				}
				if resp.Code == 0 && resp.Msg == "ok" {
					successCount.Add(1)
				}
			}
		}(i)
	}
	wg.Wait()

	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("stress error: %v", err)
		}
	}

	clientsMu.Lock()
	for _, c := range clients {
		_ = c.Stop()
	}
	clientsMu.Unlock()

	got := successCount.Load()
	elapsed := time.Since(start)
	kklog.Debugf("kkrpc stress: %d/%d calls in %v, ≈ %.0f calls/s", got, totalCalls, elapsed, float64(got)/elapsed.Seconds())
	if got < totalCalls*95/100 {
		t.Errorf("kkrpc stress: got %d < 95%% of %d", got, totalCalls)
	}
}

// TestStress_Rpc_ConnectDisconnect 快速建连断连
func TestStress_Rpc_ConnectDisconnect(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	rounds := 80
	connsPerRound := 15

	addr := freePortRpcStress(t)
	ClearRpcManagerForTest()
	RegisterReqRspMethod[testReq, testRsp]("testReqRsp")

	rpcRouter := NewRpcReceiver()
	RegistReqRspHandler(rpcRouter, "testReqRsp", func(ctx context.Context, msg *testReq, resp *testRsp) error {
		resp.Code = 0
		resp.Msg = "ok"
		return nil
	})

	opts := kknet.ApplyOptions(
		kknet.WithLogger(kklog.Nop()),
	)
	svr := NewServer(addr, opts, rpcRouter)
	if err := svr.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svr.Stop()
	waitTCPReadyRpc(t, addr, 2*time.Second)

	start := time.Now()
	for r := 0; r < rounds; r++ {
		var wg sync.WaitGroup
		for i := 0; i < connsPerRound; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				cli := NewClient(addr, opts, rpcRouter)
				_ = cli.Start()
				time.Sleep(5 * time.Millisecond)
				_ = cli.Stop()
			}()
		}
		wg.Wait()
	}
	elapsed := time.Since(start)
	totalConns := rounds * connsPerRound
	kklog.Debugf("kkrpc connect/disconnect: %d rounds × %d conns = %d total in %v, ≈ %.0f conn/s",
		rounds, connsPerRound, totalConns, elapsed, float64(totalConns)/elapsed.Seconds())
}

// TestStress_Rpc_ConcurrentSingleConn 单连接多 goroutine 并发调用
func TestStress_Rpc_ConcurrentSingleConn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	numGoroutines := 128
	callsPerGoroutine := 8888
	totalCalls := int64(numGoroutines * callsPerGoroutine)

	addr := freePortRpcStress(t)
	ClearRpcManagerForTest()
	RegisterReqRspMethod[testReq, testRsp]("testReqRsp")

	successCount := atomic.Int64{}
	rpcRouter := NewRpcReceiver()
	RegistReqRspHandler(rpcRouter, "testReqRsp", func(ctx context.Context, msg *testReq, resp *testRsp) error {
		resp.Code = 0
		resp.Msg = "ok"
		return nil
	})

	opts := kknet.ApplyOptions(
		kknet.WithLogger(kklog.Nop()),
		kknet.WithBufferSizes(4*1024, 4*1024),
	)
	svr := NewServer(addr, opts, rpcRouter)
	if err := svr.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svr.Stop()
	waitTCPReadyRpc(t, addr, 2*time.Second)

	cli := NewClient(addr, opts, rpcRouter)
	if err := startClientWithRetry(cli, 30, 10*time.Millisecond); err != nil {
		t.Fatalf("client start: %v", err)
	}
	defer cli.Stop()

	invoker := NewReqRspInvoker[testReq, testRsp](cli, 0, "testReqRsp")
	start := time.Now()
	var wg sync.WaitGroup
	errCh := make(chan error, numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for j := 0; j < callsPerGoroutine; j++ {
				req := testReq{ID: gid*10000 + j, Data: "concurrent"}
				var resp testRsp
				if err := invoker.Invoke(context.Background(), &req, CallConfig{Timeout: 5 * time.Second}, &resp); err != nil {
					errCh <- err
					return
				}
				if resp.Code == 0 && resp.Msg == "ok" {
					successCount.Add(1)
				}
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent stress error: %v", err)
		}
	}

	got := successCount.Load()
	elapsed := time.Since(start)
	kklog.Debugf("kkrpc concurrent: %d/%d calls in %v, ≈ %.0f calls/s", got, totalCalls, elapsed, float64(got)/elapsed.Seconds())
	if got < totalCalls*95/100 {
		t.Errorf("kkrpc concurrent: got %d < 95%% of %d", got, totalCalls)
	}
}

// TestStress_Rpc_InvokeNR_ManyConns_ManyCalls InvokeNR 多连接、每连接多次单向调用
func TestStress_Rpc_InvokeNR_ManyConns_ManyCalls(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	numConns := 32
	callsPerConn := 2000
	totalCalls := int64(numConns * callsPerConn)

	addr := freePortRpcStress(t)
	ClearRpcManagerForTest()
	RegisterOneWayMethod[testReq]("testOneway")

	recvCount := atomic.Int64{}
	rpcRouter := NewRpcReceiver()
	RegistOneWayHandler(rpcRouter, "testOneway", func(ctx context.Context, msg *testReq) error {
		recvCount.Add(1)
		return nil
	})

	opts := kknet.ApplyOptions(
		kknet.WithLogger(kklog.Nop()),
		kknet.WithBufferSizes(4*1024, 4*1024),
	)
	svr := NewServer(addr, opts, rpcRouter)
	if err := svr.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svr.Stop()
	waitTCPReadyRpc(t, addr, 2*time.Second)

	start := time.Now()
	var wg sync.WaitGroup
	errCh := make(chan error, numConns)
	clients := make([]*Client, 0, numConns)
	var clientsMu sync.Mutex
	connSem := make(chan struct{}, 50)

	for i := 0; i < numConns; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			connSem <- struct{}{}
			defer func() { <-connSem }()
			cli := NewClient(addr, opts, rpcRouter)
			if err := startClientWithRetry(cli, 30, 10*time.Millisecond); err != nil {
				errCh <- err
				return
			}
			clientsMu.Lock()
			clients = append(clients, cli)
			clientsMu.Unlock()

			invoker := NewOneWayInvoker[testReq](cli, 0, "testOneway")
			for j := 0; j < callsPerConn; j++ {
				req := testReq{ID: idx*1000 + j, Data: "invokenr"}
				if err := invoker.InvokeNR(context.Background(), &req, CallConfig{}); err != nil {
					errCh <- err
					return
				}
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("InvokeNR stress error: %v", err)
		}
	}

	clientsMu.Lock()
	for _, c := range clients {
		_ = c.Stop()
	}
	clientsMu.Unlock()

	// 等待服务端处理完
	deadline := time.Now().Add(5 * time.Second)
	for recvCount.Load() < totalCalls && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	got := recvCount.Load()
	elapsed := time.Since(start)
	kklog.Debugf("kkrpc InvokeNR: %d/%d received in %v, ≈ %.0f calls/s", got, totalCalls, elapsed, float64(got)/elapsed.Seconds())
	if got < totalCalls*95/100 {
		t.Errorf("kkrpc InvokeNR: got %d < 95%% of %d", got, totalCalls)
	}
}

// TestStress_Rpc_InvokeNR_ConcurrentSingleConn InvokeNR 单连接多 goroutine 并发单向调用
func TestStress_Rpc_InvokeNR_ConcurrentSingleConn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	numGoroutines := 32
	callsPerGoroutine := 8888
	totalCalls := int64(numGoroutines * callsPerGoroutine)

	addr := freePortRpcStress(t)
	ClearRpcManagerForTest()
	RegisterOneWayMethod[testReq]("testOneway")

	recvCount := atomic.Int64{}
	rpcRouter := NewRpcReceiver()
	RegistOneWayHandler(rpcRouter, "testOneway", func(ctx context.Context, msg *testReq) error {
		recvCount.Add(1)
		return nil
	})

	opts := kknet.ApplyOptions(
		kknet.WithLogger(kklog.Nop()),
		kknet.WithBufferSizes(4*1024, 4*1024),
	)
	svr := NewServer(addr, opts, rpcRouter)
	if err := svr.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svr.Stop()
	waitTCPReadyRpc(t, addr, 2*time.Second)

	cli := NewClient(addr, opts, rpcRouter)
	if err := startClientWithRetry(cli, 30, 10*time.Millisecond); err != nil {
		t.Fatalf("client start: %v", err)
	}
	defer cli.Stop()

	invoker := NewOneWayInvoker[testReq](cli, 0, "testOneway")
	start := time.Now()
	var wg sync.WaitGroup
	errCh := make(chan error, numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for j := 0; j < callsPerGoroutine; j++ {
				req := testReq{ID: gid*10000 + j, Data: "invokenr"}
				if err := invoker.InvokeNR(context.Background(), &req, CallConfig{}); err != nil {
					errCh <- err
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("InvokeNR concurrent stress error: %v", err)
		}
	}

	// 等待服务端处理完
	deadline := time.Now().Add(5 * time.Second)
	for recvCount.Load() < totalCalls && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	got := recvCount.Load()
	elapsed := time.Since(start)
	kklog.Debugf("kkrpc InvokeNR concurrent: %d/%d received in %v, ≈ %.0f calls/s", got, totalCalls, elapsed, float64(got)/elapsed.Seconds())
	if got < totalCalls*95/100 {
		t.Errorf("kkrpc InvokeNR concurrent: got %d < 95%% of %d", got, totalCalls)
	}
}
