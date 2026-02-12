package kkrpc

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
)

// setupTestRpcManager 测试前清理并注册 testReq/testRsp 相关方法
func setupTestRpcManager(t *testing.T) {
	t.Helper()
	clearRpcManager()
	RegisterReqRspMethod[testReq, testRsp]("testReqRsp")
	RegisterOneWayMethod[testReq]("testOneway")
}

type rpcProcessor struct{}

func (rp *rpcProcessor) onTestReqTestRsp(ctx context.Context, msg *testReq, resp *testRsp) error {
	//fmt.Println("remote reqrsp testReq", msg)
	resp.Code = 0
	resp.Msg = "test success"
	return nil
}

func (rp *rpcProcessor) onTestReq(ctx context.Context, msg *testReq) error {
	//fmt.Println("remote oneway testReq", msg)
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
	setupTestRpcManager(t)
	rpcRouter := NewRpcReceiver()
	rp := &rpcProcessor{}
	RegistReqRspHandler(rpcRouter, "testReqRsp", rp.onTestReqTestRsp)
	RegistOneWayHandler(rpcRouter, "testOneway", rp.onTestReq)

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

// newTestServerClientWithHandler 创建带自定义 handler 的 Server/Client，用于测试超时、错误等场景
func newTestServerClientWithHandler(t *testing.T, handler ReqRspHandlerFunc[testReq, testRsp]) (*Server, *Client) {
	t.Helper()
	setupTestRpcManager(t)
	rpcRouter := NewRpcReceiver()
	RegistReqRspHandler(rpcRouter, "testReqRsp", handler)
	return newTestServerClient(t, rpcRouter)
}

func Test_Invoke_Timeout(t *testing.T) {
	_, cli := newTestServerClientWithHandler(t, func(ctx context.Context, msg *testReq, resp *testRsp) error {
		time.Sleep(300 * time.Millisecond)
		resp.Code = 0
		resp.Msg = "late success"
		return nil
	})

	req := testReq{ID: 1, Data: "timeout"}
	var resp testRsp
	invoker := NewReqRspInvoker[testReq, testRsp](cli, 0)
	err := invoker.Invoke(context.Background(), "testReqRsp", &req, CallConfig{Timeout: 100 * time.Millisecond}, &resp)
	if !errors.Is(err, kkerrors.ErrTimeout) {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}
}

func Test_Invoke_ContextCanceled(t *testing.T) {
	_, cli := newTestServerClientWithHandler(t, func(ctx context.Context, msg *testReq, resp *testRsp) error {
		resp.Code = 0
		resp.Msg = "should not reach"
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	req := testReq{ID: 1, Data: "ctx"}
	var resp testRsp
	invoker := NewReqRspInvoker[testReq, testRsp](cli, 0)
	err := invoker.Invoke(ctx, "testReqRsp", &req, CallConfig{}, &resp)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func Test_Invoke_OnClosedClient(t *testing.T) {
	_, cli := newTestServerClientWithHandler(t, func(ctx context.Context, msg *testReq, resp *testRsp) error {
		resp.Code = 0
		resp.Msg = "ok"
		return nil
	})

	_ = cli.Stop()

	req := testReq{ID: 1, Data: "closed"}
	var resp testRsp
	invoker := NewReqRspInvoker[testReq, testRsp](cli, 0)
	err := invoker.Invoke(context.Background(), "testReqRsp", &req, CallConfig{}, &resp)
	if !errors.Is(err, kkerrors.ErrConnClosed) {
		t.Fatalf("expected ErrConnClosed, got %v", err)
	}
}

func Test_InvokeAsync_Success(t *testing.T) {
	_, cli := newTestServerClientWithHandler(t, func(ctx context.Context, msg *testReq, resp *testRsp) error {
		resp.Code = 0
		resp.Msg = "async success"
		return nil
	})

	req := testReq{ID: 1, Data: "async"}
	invoker := NewReqRspInvoker[testReq, testRsp](cli, 0)

	done := make(chan struct{})
	var gotResp *testRsp
	var gotErr error

	err := invoker.InvokeAsync(context.Background(), "testReqRsp", &req, CallConfig{}, func(r *testRsp, e error) {
		gotResp = r
		gotErr = e
		close(done)
	})
	if err != nil {
		t.Fatalf("InvokeAsync: %v", err)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("InvokeAsync timeout waiting for callback")
	}

	if gotErr != nil {
		t.Fatalf("unexpected error: %v", gotErr)
	}
	if gotResp == nil || gotResp.Code != 0 || gotResp.Msg != "async success" {
		t.Fatalf("unexpected resp: %+v", gotResp)
	}
}

func Test_InvokeAsync_Error(t *testing.T) {
	_, cli := newTestServerClientWithHandler(t, func(ctx context.Context, msg *testReq, resp *testRsp) error {
		return errors.New("boom")
	})

	req := testReq{ID: 1, Data: "async-err"}
	invoker := NewReqRspInvoker[testReq, testRsp](cli, 0)

	done := make(chan struct{})
	var gotResp *testRsp
	var gotErr error

	err := invoker.InvokeAsync(context.Background(), "testReqRsp", &req, CallConfig{}, func(r *testRsp, e error) {
		gotResp = r
		gotErr = e
		close(done)
	})
	if err != nil {
		t.Fatalf("InvokeAsync: %v", err)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("InvokeAsync timeout waiting for callback")
	}

	if gotResp != nil {
		t.Fatalf("expected nil resp on error, got %+v", gotResp)
	}
	if gotErr == nil || !strings.Contains(gotErr.Error(), "远程方法执行失败") {
		t.Fatalf("unexpected error: %v", gotErr)
	}
}

func Test_InvokeAsync_Timeout(t *testing.T) {
	_, cli := newTestServerClientWithHandler(t, func(ctx context.Context, msg *testReq, resp *testRsp) error {
		time.Sleep(300 * time.Millisecond)
		resp.Code = 0
		resp.Msg = "late"
		return nil
	})

	req := testReq{ID: 1, Data: "async-timeout"}
	invoker := NewReqRspInvoker[testReq, testRsp](cli, 0)

	done := make(chan struct{})
	var callCount int
	var gotErr error

	err := invoker.InvokeAsync(context.Background(), "testReqRsp", &req, CallConfig{Timeout: 80 * time.Millisecond}, func(r *testRsp, e error) {
		callCount++
		gotErr = e
		close(done)
	})
	if err != nil {
		t.Fatalf("InvokeAsync: %v", err)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("InvokeAsync timeout waiting for callback")
	}

	if callCount != 1 {
		t.Fatalf("expected callback once, got %d", callCount)
	}
	if !errors.Is(gotErr, kkerrors.ErrTimeout) {
		t.Fatalf("expected ErrTimeout, got %v", gotErr)
	}
}

func Test_InvokeNR(t *testing.T) {
	setupTestRpcManager(t)
	called := make(chan struct{}, 1)
	rpcRouter := NewRpcReceiver()
	RegistOneWayHandler(rpcRouter, "testOneway", func(ctx context.Context, msg *testReq) error {
		select {
		case called <- struct{}{}:
		default:
		}
		return nil
	})

	_, cli := newTestServerClient(t, rpcRouter)

	req := testReq{ID: 1, Data: "oneway"}
	invoker := NewOneWayInvoker[testReq](cli, 0)
	if err := invoker.InvokeNR(context.Background(), "testOneway", &req, CallConfig{}); err != nil {
		t.Fatalf("InvokeNR: %v", err)
	}

	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("InvokeNR handler not called")
	}
}

func Test_ServerInvoker(t *testing.T) {
	svr, cli := newTestServerClientWithHandler(t, func(ctx context.Context, msg *testReq, resp *testRsp) error {
		resp.Code = 0
		resp.Msg = "test success"
		return nil
	})

	// 获取 server 端的 connId
	var connId kknet.CONN_ID
	svr.tcp.GetConnManager().RangeAllConns(func(id kknet.CONN_ID, conn kknet.IConn) bool {
		connId = id
		return false
	})

	req := testReq{ID: 2, Data: "test2"}
	var resp testRsp
	invoker := NewReqRspInvoker[testReq, testRsp](svr, connId)
	err := invoker.Invoke(context.Background(), "testReqRsp", &req, CallConfig{}, &resp)
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if resp.Code != 0 || resp.Msg != "test success" {
		t.Fatalf("unexpected response: code=%d msg=%s", resp.Code, resp.Msg)
	}

	_ = cli
}
