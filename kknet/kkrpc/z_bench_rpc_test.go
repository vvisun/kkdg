package kkrpc

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet"
)

func Benchmark_InvokeUnary(b *testing.B) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	rpcRouter := NewRpcReceiver()
	RegistReqRspHandler(rpcRouter, "test", func(ctx context.Context, msg *testReq, resp *testRsp) error {
		resp.Code = 0
		resp.Msg = "test success"
		return nil
	})

	svr := NewServer(addr, kknet.DefaultOptions(), rpcRouter)
	err = svr.Start()
	if err != nil {
		b.Fatalf("start server: %v", err)
	}
	defer svr.Stop()

	cli := NewClient(addr, kknet.DefaultOptions(), rpcRouter)
	err = cli.Start()
	if err != nil {
		b.Fatalf("start client: %v", err)
	}
	defer cli.Stop()

	time.Sleep(500 * time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var req testReq = testReq{
			ID:   1,
			Data: "test",
		}
		var resp testRsp
		invoker := NewClientInvoker[testReq, testRsp](cli)
		err = invoker.Invoke(context.Background(), "test", &req, CallConfig{}, &resp)
		if err != nil {
			b.Fatalf("invoke: %v", err)
		}
		if resp.Code != 0 || resp.Msg != "test success" {
			b.Fatalf("unexpected response: code=%d msg=%s", resp.Code, resp.Msg)
		}
	}
}
