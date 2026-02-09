package kkrpc

import (
	"context"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet"
)

func Test_Client_Request(t *testing.T) {
	rpcRouter := NewRpcReceiver()
	RegistRpcHandler(rpcRouter, "test", func(ctx context.Context, msg *testMsg, resp *testRsp) error {
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

	time.Sleep(2 * time.Second)

	var req testMsg = testMsg{
		ID:   1,
		Data: "test",
	}
	var resp testRsp
	invoker := RpcInvoker[testMsg, testRsp]{c: cli}
	err = invoker.Invoke(context.Background(), "test", &req, CallConfig{}, &resp)
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}

	time.Sleep(2 * time.Second)
}
