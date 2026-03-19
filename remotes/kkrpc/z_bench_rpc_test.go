package kkrpc

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/kklog"
)

func Benchmark_InvokeUnary(b *testing.B) {
	methodMgr := NewMethodManager(gStreamTool, gFrameCodec, gPayloadCodec)
	RegisterReqRspMethod[testReq, testRsp](methodMgr)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	rpcRouter := NewRpcReceiver(DefaultRpcOption(), methodMgr)
	RegistReqRspHandler(rpcRouter, func(ctx context.Context, msg *testReq, resp *testRsp, connId kknet.CONN_ID) error {
		resp.Code = 0
		resp.Msg = "test success"
		return nil
	})

	svr := NewServer(addr, kknet.ApplyOptions(
		kknet.WithLogger(kklog.Nop()),
	), rpcRouter)
	if err := svr.Start(); err != nil {
		b.Fatalf("start server: %v", err)
	}
	defer svr.Stop()

	cli := NewClient(addr, kknet.ApplyOptions(
		kknet.WithLogger(kklog.Nop()),
	), rpcRouter)
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
		invoker, err := NewReqRspInvoker[testReq, testRsp](cli)
		if err != nil {
			b.Fatalf("create reqrsp invoker: %v", err)
		}
		if err := invoker.Invoke(context.Background(), &req, CallConfig{}, &resp); err != nil {
			if err != kkerrors.ErrRpcQueueFull {
				b.Fatalf("invoke: %v", err)
			}
		}
	}

	fmt.Printf("stats: %+v\n", cli.Stats())
}

func Benchmark_InvokeUnary_ReuseInvoker(b *testing.B) {
	methodMgr := NewMethodManager(gStreamTool, gFrameCodec, gPayloadCodec)
	RegisterReqRspMethod[testReq, testRsp](methodMgr)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	rpcRouter := NewRpcReceiver(DefaultRpcOption(), methodMgr)
	RegistReqRspHandler(rpcRouter, func(ctx context.Context, msg *testReq, resp *testRsp, connId kknet.CONN_ID) error {
		resp.Code = 0
		resp.Msg = "test success"
		return nil
	})

	svr := NewServer(addr, kknet.ApplyOptions(
		kknet.WithLogger(kklog.Nop()),
	), rpcRouter)
	if err := svr.Start(); err != nil {
		b.Fatalf("start server: %v", err)
	}
	defer svr.Stop()

	cli := NewClient(addr, kknet.ApplyOptions(
		kknet.WithLogger(kklog.Nop()),
	), rpcRouter)
	if err := cli.Start(); err != nil {
		b.Fatalf("start client: %v", err)
	}
	defer cli.Stop()

	time.Sleep(200 * time.Millisecond)

	invoker, err := NewReqRspInvoker[testReq, testRsp](cli)
	if err != nil {
		b.Fatalf("create reqrsp invoker: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req := testReq{ID: i, Data: "test"}
		var resp testRsp
		if err := invoker.Invoke(context.Background(), &req, CallConfig{}, &resp); err != nil {
			if err != kkerrors.ErrRpcQueueFull {
				b.Fatalf("invoke: %v", err)
			}
		}
	}

	fmt.Printf("stats: %+v\n", invoker.sender.Stats())
}

// Benchmark_InvokeUnary_Parallel 单连接并发调用，多个 goroutine 共享同一 client，测试真实并发下的 req/resp 匹配与编解码。
func Benchmark_InvokeUnary_Parallel(b *testing.B) {
	methodMgr := NewMethodManager(gStreamTool, gFrameCodec, gPayloadCodec)
	RegisterReqRspMethod[testReq, testRsp](methodMgr)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	rpcRouter := NewRpcReceiver(DefaultRpcOption(), methodMgr)
	RegistReqRspHandler(rpcRouter, func(ctx context.Context, msg *testReq, resp *testRsp, connId kknet.CONN_ID) error {
		resp.Code = 0
		resp.Msg = "test success"
		return nil
	})

	svr := NewServer(addr, kknet.ApplyOptions(
		kknet.WithLogger(kklog.Nop()),
	), rpcRouter)
	if err := svr.Start(); err != nil {
		b.Fatalf("start server: %v", err)
	}
	defer svr.Stop()

	cli := NewClient(addr, kknet.ApplyOptions(
		kknet.WithLogger(kklog.Nop()),
	), rpcRouter)
	if err := cli.Start(); err != nil {
		b.Fatalf("start client: %v", err)
	}
	defer cli.Stop()

	invoker, err := NewReqRspInvoker[testReq, testRsp](cli)
	if err != nil {
		b.Fatalf("create reqrsp invoker: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			req := testReq{ID: i, Data: "test"}
			var resp testRsp
			if err := invoker.Invoke(context.Background(), &req, CallConfig{}, &resp); err != nil {
				if err != kkerrors.ErrRpcQueueFull {
					b.Fatalf("invoke: %v", err)
				}
			}
			i++
		}
	})

	fmt.Printf("stats: %+v\n", invoker.sender.Stats())
}
