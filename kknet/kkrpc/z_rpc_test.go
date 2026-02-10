package kkrpc

import (
	"context"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet"
)

func Test_Client_Request(t *testing.T) {
	rpcRouter := NewRpcReceiver()
	RegistRpcHandler(rpcRouter, "test", func(ctx context.Context, msg *testReq, resp *testRsp) error {
		resp.Code = 0
		resp.Msg = "test success"
		return nil
	})

	svr := NewServer("localhost:8080", kknet.DefaultOptions(), rpcRouter)
	err := svr.Start()
	if err != nil {
		t.Fatalf("start server: %v", err)
	}

	cli := NewClient("localhost:8080", kknet.DefaultOptions(), rpcRouter)
	err = cli.Start()
	if err != nil {
		t.Fatalf("start client: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	var req testReq = testReq{
		ID:   1,
		Data: "test",
	}
	var resp testRsp
	invoker := NewRpcInvoker[testReq, testRsp](cli)
	err = invoker.Invoke(context.Background(), "test", &req, CallConfig{}, &resp)
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if resp.Code != 0 || resp.Msg != "test success" {
		t.Fatalf("unexpected response: code=%d msg=%s", resp.Code, resp.Msg)
	}
}
