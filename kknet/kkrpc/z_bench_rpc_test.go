package kkrpc

import (
	"context"
	"net"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

func Benchmark_InvokeUnary(b *testing.B) {
	ClearRpcManagerForTest()
	RegisterReqRspMethod[testReq, testRsp]("testReqRsp")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	rpcRouter := NewRpcReceiver()
	RegistReqRspHandler(rpcRouter, "testReqRsp", func(ctx context.Context, msg *testReq, resp *testRsp) error {
		resp.Code = 0
		resp.Msg = "test success"
		return nil
	})

	svr := NewServer(addr, kknet.DefaultOptions(), rpcRouter)
	if err := svr.Start(); err != nil {
		b.Fatalf("start server: %v", err)
	}
	defer svr.Stop()

	cli := NewClient(addr, kknet.DefaultOptions(), rpcRouter)
	if err := cli.Start(); err != nil {
		b.Fatalf("start client: %v", err)
	}
	defer cli.Stop()

	time.Sleep(200 * time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req := testReq{ID: 1, Data: "test"}
		var resp testRsp
		invoker := NewReqRspInvoker[testReq, testRsp](cli, 0)
		if err := invoker.Invoke(context.Background(), "testReqRsp", &req, CallConfig{}, &resp); err != nil {
			b.Fatalf("invoke: %v", err)
		}
	}
}

func Benchmark_InvokeUnary_ReuseInvoker(b *testing.B) {
	ClearRpcManagerForTest()
	RegisterReqRspMethod[testReq, testRsp]("testReqRsp")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	rpcRouter := NewRpcReceiver()
	RegistReqRspHandler(rpcRouter, "testReqRsp", func(ctx context.Context, msg *testReq, resp *testRsp) error {
		resp.Code = 0
		resp.Msg = "test success"
		return nil
	})

	svr := NewServer(addr, kknet.DefaultOptions(), rpcRouter)
	if err := svr.Start(); err != nil {
		b.Fatalf("start server: %v", err)
	}
	defer svr.Stop()

	cli := NewClient(addr, kknet.DefaultOptions(), rpcRouter)
	if err := cli.Start(); err != nil {
		b.Fatalf("start client: %v", err)
	}
	defer cli.Stop()

	time.Sleep(200 * time.Millisecond)

	invoker := NewReqRspInvoker[testReq, testRsp](cli, 0)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req := testReq{ID: i, Data: "test"}
		var resp testRsp
		if err := invoker.Invoke(context.Background(), "testReqRsp", &req, CallConfig{}, &resp); err != nil {
			b.Fatalf("invoke: %v", err)
		}
	}
}

func Benchmark_EncodeRpcFrame(b *testing.B) {
	ClearRpcManagerForTest()
	RegisterReqRspMethod[testReq, testRsp]("testReqRsp")

	msg := &testReq{ID: 1, Data: "benchmark"}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		bb, err := EncodeRpcFrame(FrameTypeRequest, uint64(i+1), "testReqRsp", msg)
		if err != nil {
			b.Fatalf("encode: %v", err)
		}
		kkbuffer.Put(bb)
	}
}

// Benchmark_InvokeUnary_Parallel 多连接并发调用。每个 goroutine 独占一个 client，无共享连接。
func Benchmark_InvokeUnary_Parallel(b *testing.B) {
	ClearRpcManagerForTest()
	RegisterReqRspMethod[testReq, testRsp]("testReqRsp")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	rpcRouter := NewRpcReceiver()
	RegistReqRspHandler(rpcRouter, "testReqRsp", func(ctx context.Context, msg *testReq, resp *testRsp) error {
		resp.Code = 0
		resp.Msg = "test success"
		return nil
	})

	svr := NewServer(addr, kknet.DefaultOptions(), rpcRouter)
	if err := svr.Start(); err != nil {
		b.Fatalf("start server: %v", err)
	}
	defer svr.Stop()

	numConns := runtime.GOMAXPROCS(0)
	if numConns < 4 {
		numConns = 4
	}
	if numConns > 32 {
		numConns = 32
	}

	clients := make([]*Client, numConns)
	invokers := make([]ReqRspInvoker[testReq, testRsp], numConns)

	for i := 0; i < numConns; i++ {
		cli := NewClient(addr, kknet.DefaultOptions(), rpcRouter)
		if err := cli.Start(); err != nil {
			b.Fatalf("start client: %v", err)
		}
		clients[i] = cli
		invokers[i] = NewReqRspInvoker[testReq, testRsp](cli, 0)
	}
	defer func() {
		for _, c := range clients {
			c.Stop()
		}
	}()

	time.Sleep(200 * time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()
	var parallelLaneIdx uint32
	b.RunParallel(func(pb *testing.PB) {
		// 每个 goroutine 绑定到固定 client，避免共享连接
		myLane := int(atomic.AddUint32(&parallelLaneIdx, 1)-1) % numConns
		invoker := invokers[myLane]
		i := 0
		for pb.Next() {
			req := testReq{ID: i, Data: "test"}
			var resp testRsp
			if err := invoker.Invoke(context.Background(), "testReqRsp", &req, CallConfig{}, &resp); err != nil {
				b.Fatalf("invoke: %v", err)
			}
			i++
		}
	})
}
