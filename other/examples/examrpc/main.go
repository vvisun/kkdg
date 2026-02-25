package main

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/remotes/kkrpc"
)

// examrpc 演示基于 TCP 的 RPC 用法（请求响应 + 单向）
// cd other/examples/examrpc; go run main.go

func main() {
	addr := freePort()
	runRpcDemo(addr)
}

func freePort() string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	a := ln.Addr().String()
	_ = ln.Close()
	return a
}

type EchoReq struct {
	Msg string
}

type EchoRsp struct {
	Reply string
}

type PingReq struct {
	From string
}

func runRpcDemo(addr string) {
	kkrpc.RegisterReqRspMethod[EchoReq, EchoRsp]("Echo")
	kkrpc.RegisterOneWayMethod[PingReq]("Ping")

	rpcRouter := kkrpc.NewRpcReceiver()
	kkrpc.RegistReqRspHandler(rpcRouter, "Echo", onEcho)
	kkrpc.RegistOneWayHandler(rpcRouter, "Ping", onPing)

	opts := kknet.ApplyOptions(kknet.WithIsNeedReconnect(false))

	svr := kkrpc.NewServer(addr, opts, rpcRouter)
	if err := svr.Start(); err != nil {
		panic(err)
	}
	defer svr.Stop()

	cli := kkrpc.NewClient(addr, opts, rpcRouter)
	if err := cli.Start(); err != nil {
		panic(err)
	}
	defer cli.Stop()

	time.Sleep(100 * time.Millisecond)

	// 请求-响应
	req := &EchoReq{Msg: "hello examrpc"}
	var rsp EchoRsp
	invoker := kkrpc.NewReqRspInvoker[EchoReq, EchoRsp](cli, 0, "Echo")
	if err := invoker.Invoke(context.Background(), req, kkrpc.CallConfig{}, &rsp); err != nil {
		panic(err)
	}
	fmt.Printf("Echo response: %q\n", rsp.Reply)

	// 单向
	pingReq := &PingReq{From: "examrpc"}
	oneWayInvoker := kkrpc.NewOneWayInvoker[PingReq](cli, 0, "Ping")
	if err := oneWayInvoker.InvokeNR(context.Background(), pingReq, kkrpc.CallConfig{}); err != nil {
		panic(err)
	}
	fmt.Println("Ping sent")

	// 异步调用
	done := make(chan struct{})
	asyncReq := &EchoReq{Msg: "async call"}
	_ = invoker.InvokeAsync(context.Background(), asyncReq, kkrpc.CallConfig{}, func(r *EchoRsp, e error) {
		if e != nil {
			panic(e)
		}
		fmt.Printf("Async Echo response: %q\n", r.Reply)
		close(done)
	})
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		panic("async timeout")
	}

	fmt.Println("examrpc demo ok")
}

func onEcho(ctx context.Context, req *EchoReq, rsp *EchoRsp) error {
	rsp.Reply = "echo: " + req.Msg
	return nil
}

func onPing(ctx context.Context, req *PingReq) error {
	fmt.Printf("[server] Ping from %s\n", req.From)
	return nil
}
