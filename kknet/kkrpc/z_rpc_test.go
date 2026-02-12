package kkrpc

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet"
)

type rpcProcessor struct{}

func (rp *rpcProcessor) onTestReqTestRsp(ctx context.Context, msg *testReq, resp *testRsp) error {
	fmt.Println("remote reqrsp testReq", msg)
	resp.Code = 0
	resp.Msg = "test success"
	return nil
}

func (rp *rpcProcessor) onTestReq(ctx context.Context, msg *testReq) error {
	fmt.Println("remote oneway testReq", msg)
	return nil
}

func newTestServerClient(t *testing.T, rpcRouter *RpcReceiver) (*Server, *Client) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	svr := NewServer(addr, kknet.DefaultOptions(), rpcRouter)
	if err := svr.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(func() { _ = svr.Stop() })

	cli := NewClient(addr, kknet.DefaultOptions(), rpcRouter)
	if err := cli.Start(); err != nil {
		t.Fatalf("start client: %v", err)
	}
	t.Cleanup(func() { _ = cli.Stop() })

	time.Sleep(100 * time.Millisecond)
	return svr, cli
}

func Test_RpcProcessor(t *testing.T) {
	rpcRouter := NewRpcReceiver()
	rp := &rpcProcessor{}
	RegistReqRspHandler(rpcRouter, "testReqRsp", rp.onTestReqTestRsp)
	RegistOneWayHandler(rpcRouter, "testOneway", rp.onTestReq)
	RegisterOneWayMethod[testReq]("testOneway")
	RegisterReqRspMethod[testReq, testRsp]("testReqRsp")

	_, cli := newTestServerClient(t, rpcRouter)

	var req testReq = testReq{
		ID:   1,
		Data: "test",
	}
	var resp testRsp
	reqRspInvoker := NewReqRspInvoker[testReq, testRsp](cli, 0)
	err := reqRspInvoker.Invoke(context.Background(), "testReqRsp", &req, CallConfig{}, &resp)
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if resp.Code != 0 || resp.Msg != "test success" {
		t.Fatalf("unexpected response: code=%d msg=%s", resp.Code, resp.Msg)
	}

	oneWayInvoker := NewOneWayInvoker[testReq](cli, 0)
	err = oneWayInvoker.InvokeNR(context.Background(), "testOneway", &req, CallConfig{})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
}
