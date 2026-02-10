package kkrpc

import (
	"context"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet"
)

func Benchmark_InvokeUnary(b *testing.B) {
	rpcRouter := NewRpcReceiver()
	RegistRpcHandler(rpcRouter, "test", func(ctx context.Context, msg *testReq, resp *testRsp) error {
		resp.Code = 0
		resp.Msg = "test success"
		return nil
	})

	svr := NewServer("localhost:8080", kknet.DefaultOptions(), rpcRouter)
	err := svr.Start()
	if err != nil {
		b.Fatalf("start server: %v", err)
	}

	cli := NewClient("localhost:8080", kknet.DefaultOptions(), rpcRouter)
	err = cli.Start()
	if err != nil {
		b.Fatalf("start client: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var req testReq = testReq{
			ID:   1,
			Data: "test",
		}
		var resp testRsp
		invoker := NewRpcInvoker[testReq, testRsp](cli)
		err = invoker.Invoke(context.Background(), "test", &req, CallConfig{}, &resp)
		if err != nil {
			b.Fatalf("invoke: %v", err)
		}
		if resp.Code != 0 || resp.Msg != "test success" {
			b.Fatalf("unexpected response: code=%d msg=%s", resp.Code, resp.Msg)
		}
	}
}
