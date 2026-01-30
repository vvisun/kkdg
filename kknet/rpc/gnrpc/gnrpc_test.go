package gnrpc

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/vvisun/kkdg/proto/pbbase"
	"github.com/vvisun/kkdg/utils/xnet"
	"google.golang.org/protobuf/proto"
)

func TestGNRPC_UnaryEcho(t *testing.T) {
	port, err := xnet.AssignRandPort("127.0.0.1")
	if err != nil {
		t.Fatalf("assign port: %v", err)
	}
	addr := "127.0.0.1:" + strconv.Itoa(port)

	svr := NewServer(addr)
	svr.Register("echo", UnaryHandler(func(ctx context.Context, req []byte) ([]byte, error) {
		_ = ctx
		out := make([]byte, 0, len(req)+4)
		out = append(out, []byte("pong")...)
		out = append(out, req...)
		return out, nil
	}))
	if err := svr.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer svr.Stop()

	cli := NewClient(addr)
	if err := cli.Connect(); err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := cli.Invoke(ctx, "echo", []byte("123"))
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if string(resp) != "pong123" {
		t.Fatalf("unexpected resp: %q", string(resp))
	}
}

func TestGNRPC_InterceptorAndProto(t *testing.T) {
	port, err := xnet.AssignRandPort("127.0.0.1")
	if err != nil {
		t.Fatalf("assign port: %v", err)
	}
	addr := "127.0.0.1:" + strconv.Itoa(port)

	svr := NewServer(addr)
	var serverInterceptorCalled bool
	svr.UseInterceptor(func(ctx context.Context, method string, req []byte, handler Handler) ([]byte, error) {
		serverInterceptorCalled = true
		_ = req
		if method == "" {
			t.Fatalf("empty method")
		}
		return handler(ctx, req) // do not mutate bytes for proto payload
	})
	svr.RegisterProto("pbbase.String/echo", func() proto.Message { return &pbbase.String{} }, func(ctx context.Context, req proto.Message) (proto.Message, error) {
		_ = ctx
		in := req.(*pbbase.String)
		return &pbbase.String{Value: "ok:" + in.Value}, nil
	})
	if err := svr.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer svr.Stop()

	cli := NewClient(addr)
	var clientInterceptorCalled bool
	cli.UseInterceptor(func(ctx context.Context, method string, req []byte, invoker Invoker) ([]byte, error) {
		clientInterceptorCalled = true
		if method == "" {
			t.Fatalf("empty method")
		}
		return invoker(ctx, method, req) // do not mutate bytes for proto payload
	})
	if err := cli.Connect(); err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var out pbbase.String
	if err := cli.InvokeProto(ctx, "pbbase.String/echo", &pbbase.String{Value: "X"}, &out); err != nil {
		t.Fatalf("invoke proto: %v", err)
	}
	if out.Value != "ok:X" {
		t.Fatalf("unexpected proto resp: %q", out.Value)
	}
	if !serverInterceptorCalled {
		t.Fatalf("server interceptor not called")
	}
	if !clientInterceptorCalled {
		t.Fatalf("client interceptor not called")
	}
}

func TestGNRPC_MetadataHeader(t *testing.T) {
	port, err := xnet.AssignRandPort("127.0.0.1")
	if err != nil {
		t.Fatalf("assign port: %v", err)
	}
	addr := "127.0.0.1:" + strconv.Itoa(port)

	svr := NewServer(addr)
	svr.RegisterProto("pbbase.String/headers", func() proto.Message { return &pbbase.String{} }, func(ctx context.Context, req proto.Message) (proto.Message, error) {
		_ = req
		md, ok := FromIncomingContext(ctx)
		if !ok {
			return nil, Status(CodeInvalidArgument, "missing metadata")
		}
		if md["x-test"] != "123" {
			return nil, Status(CodeInvalidArgument, "bad header")
		}
		return &pbbase.String{Value: "ok"}, nil
	})
	if err := svr.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer svr.Stop()

	cli := NewClient(addr)
	if err := cli.Connect(); err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var out pbbase.String
	if err := cli.InvokeProtoWithOptions(ctx, "pbbase.String/headers", &pbbase.String{Value: "x"}, &out, WithHeader("x-test", "123")); err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if out.Value != "ok" {
		t.Fatalf("unexpected resp: %q", out.Value)
	}
}

func TestGNRPC_ResponseHeaderTrailer(t *testing.T) {
	port, err := xnet.AssignRandPort("127.0.0.1")
	if err != nil {
		t.Fatalf("assign port: %v", err)
	}
	addr := "127.0.0.1:" + strconv.Itoa(port)

	svr := NewServer(addr)
	svr.RegisterProto("pbbase.String/meta", func() proto.Message { return &pbbase.String{} }, func(ctx context.Context, req proto.Message) (proto.Message, error) {
		_ = req
		SetHeader(ctx, "x-h", "hv")
		SetTrailer(ctx, "x-t", "tv")
		return &pbbase.String{Value: "ok"}, nil
	})
	if err := svr.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer svr.Stop()

	cli := NewClient(addr)
	if err := cli.Connect(); err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var rh, rt MD
	var out pbbase.String
	if err := cli.InvokeProtoWithOptions(
		ctx,
		"pbbase.String/meta",
		&pbbase.String{Value: "x"},
		&out,
		WithResponseHeaders(&rh),
		WithResponseTrailers(&rt),
	); err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if out.Value != "ok" {
		t.Fatalf("unexpected resp: %q", out.Value)
	}
	if rh["x-h"] != "hv" {
		t.Fatalf("bad resp header: %#v", rh)
	}
	if rt["x-t"] != "tv" {
		t.Fatalf("bad resp trailer: %#v", rt)
	}
}

