package kkrpc

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet"
)

// newTestServerClient 创建一个临时地址的 kkrpc Server/Client，用于单测。
func newTestServerClient(t *testing.T, handler RpcHandlerFunc[testReq, testRsp]) (*Server, *Client) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	rpcRouter := NewRpcReceiver()
	RegistRpcHandler(rpcRouter, "test", handler)

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

	// 简单等待连接建立
	time.Sleep(100 * time.Millisecond)
	return svr, cli
}

func Test_Client_Request(t *testing.T) {
	_, cli := newTestServerClient(t, func(ctx context.Context, msg *testReq, resp *testRsp) error {
		resp.Code = 0
		resp.Msg = "test success"
		return nil
	})

	var req testReq = testReq{
		ID:   1,
		Data: "test",
	}
	var resp testRsp
	invoker := NewRpcInvoker[testReq, testRsp](cli)
	err := invoker.Invoke(context.Background(), "test", &req, CallConfig{}, &resp)
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if resp.Code != 0 || resp.Msg != "test success" {
		t.Fatalf("unexpected response: code=%d msg=%s", resp.Code, resp.Msg)
	}
}

// 超时：handler 故意 sleep 超过超时时间，期望 ErrTimeout
func Test_Invoke_Timeout(t *testing.T) {
	_, cli := newTestServerClient(t, func(ctx context.Context, msg *testReq, resp *testRsp) error {
		time.Sleep(300 * time.Millisecond)
		resp.Code = 0
		resp.Msg = "late success"
		return nil
	})

	req := testReq{ID: 1, Data: "timeout"}
	var resp testRsp
	invoker := NewRpcInvoker[testReq, testRsp](cli)
	err := invoker.Invoke(context.Background(), "test", &req, CallConfig{timeout: 100 * time.Millisecond}, &resp)
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}
}

// ctx 取消：调用前取消 context，期望返回 context.Canceled
func Test_Invoke_ContextCanceled(t *testing.T) {
	_, cli := newTestServerClient(t, func(ctx context.Context, msg *testReq, resp *testRsp) error {
		resp.Code = 0
		resp.Msg = "should not reach"
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	req := testReq{ID: 1, Data: "ctx"}
	var resp testRsp
	invoker := NewRpcInvoker[testReq, testRsp](cli)
	err := invoker.Invoke(ctx, "test", &req, CallConfig{}, &resp)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

// 连接关闭后调用：期望 ErrConnClosed
func Test_Invoke_OnClosedClient(t *testing.T) {
	_, cli := newTestServerClient(t, func(ctx context.Context, msg *testReq, resp *testRsp) error {
		resp.Code = 0
		resp.Msg = "ok"
		return nil
	})

	// 主动关闭
	_ = cli.Stop()

	req := testReq{ID: 1, Data: "closed"}
	var resp testRsp
	invoker := NewRpcInvoker[testReq, testRsp](cli)
	err := invoker.Invoke(context.Background(), "test", &req, CallConfig{}, &resp)
	if !errors.Is(err, ErrConnClosed) {
		t.Fatalf("expected ErrConnClosed, got %v", err)
	}
}

// 异步调用成功路径
func Test_InvokeAsync_Success(t *testing.T) {
	_, cli := newTestServerClient(t, func(ctx context.Context, msg *testReq, resp *testRsp) error {
		resp.Code = 0
		resp.Msg = "async success"
		return nil
	})

	req := testReq{ID: 1, Data: "async"}
	invoker := NewRpcInvoker[testReq, testRsp](cli)

	done := make(chan struct{})
	var gotResp *testRsp
	var gotErr error

	err := invoker.InvokeAsync(context.Background(), "test", &req, CallConfig{}, func(r *testRsp, e error) {
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

// 异步调用错误路径：handler 返回错误，期望 callback 收到 err
func Test_InvokeAsync_Error(t *testing.T) {
	_, cli := newTestServerClient(t, func(ctx context.Context, msg *testReq, resp *testRsp) error {
		return errors.New("boom")
	})

	req := testReq{ID: 1, Data: "async-err"}
	invoker := NewRpcInvoker[testReq, testRsp](cli)

	done := make(chan struct{})
	var gotResp *testRsp
	var gotErr error

	err := invoker.InvokeAsync(context.Background(), "test", &req, CallConfig{}, func(r *testRsp, e error) {
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

// 单向调用：仅验证不返回错误且 handler 被调用
func Test_InvokeNR(t *testing.T) {
	called := make(chan struct{}, 1)
	_, cli := newTestServerClient(t, func(ctx context.Context, msg *testReq, resp *testRsp) error {
		select {
		case called <- struct{}{}:
		default:
		}
		return nil
	})

	req := testReq{ID: 1, Data: "oneway"}
	invoker := NewOneWayInvoker[testReq](cli)
	if err := invoker.InvokeNR(context.Background(), "test", &req, CallConfig{}); err != nil {
		t.Fatalf("InvokeNR: %v", err)
	}

	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("InvokeNR handler not called")
	}
}
