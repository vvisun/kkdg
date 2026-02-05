package gnrpc

import (
	"context"
	"strconv"
	"testing"

	"github.com/vvisun/kkdg/proto/pbbase"
	"github.com/vvisun/kkdg/utils/xnet"
	"google.golang.org/protobuf/proto"
)

// BenchmarkGNRPC_InvokeUnary measures unary byte-based RPC throughput.
func BenchmarkGNRPC_InvokeUnary(b *testing.B) {
	port, err := xnet.AssignRandPort("127.0.0.1")
	if err != nil {
		b.Fatalf("assign port: %v", err)
	}
	addr := "127.0.0.1:" + strconv.Itoa(port)

	svr := NewServer(addr)
	svr.Register("echo", UnaryHandler(func(ctx context.Context, req []byte) ([]byte, error) {
		_ = ctx
		// echo back the payload
		return req, nil
	}))
	if err := svr.Start(); err != nil {
		b.Fatalf("server start: %v", err)
	}
	defer svr.Stop()

	cli := NewClient(addr)
	if err := cli.Connect(); err != nil {
		b.Fatalf("client connect: %v", err)
	}
	defer cli.Close()

	payload := []byte("hello")
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := cli.Invoke(ctx, "echo", payload); err != nil {
			b.Fatalf("invoke: %v", err)
		}
	}
}

// BenchmarkGNRPC_InvokeUnary_Parallel measures unary RPC throughput under parallel clients.
func BenchmarkGNRPC_InvokeUnary_Parallel(b *testing.B) {
	port, err := xnet.AssignRandPort("127.0.0.1")
	if err != nil {
		b.Fatalf("assign port: %v", err)
	}
	addr := "127.0.0.1:" + strconv.Itoa(port)

	svr := NewServer(addr)
	svr.Register("echo", UnaryHandler(func(ctx context.Context, req []byte) ([]byte, error) {
		_ = ctx
		return req, nil
	}))
	if err := svr.Start(); err != nil {
		b.Fatalf("server start: %v", err)
	}
	defer svr.Stop()

	cli := NewClient(addr)
	if err := cli.Connect(); err != nil {
		b.Fatalf("client connect: %v", err)
	}
	defer cli.Close()

	payload := []byte("hello")
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := cli.Invoke(ctx, "echo", payload); err != nil {
				b.Fatalf("invoke: %v", err)
			}
		}
	})
}

// BenchmarkGNRPC_InvokeProto measures proto-based RPC throughput.
func BenchmarkGNRPC_InvokeProto(b *testing.B) {
	port, err := xnet.AssignRandPort("127.0.0.1")
	if err != nil {
		b.Fatalf("assign port: %v", err)
	}
	addr := "127.0.0.1:" + strconv.Itoa(port)

	svr := NewServer(addr)
	svr.RegisterProto("pbbase.String/echo_bench",
		func() proto.Message { return &pbbase.String{} },
		func(ctx context.Context, req proto.Message) (proto.Message, error) {
			_ = ctx
			in := req.(*pbbase.String)
			return &pbbase.String{Value: in.Value}, nil
		},
	)
	if err := svr.Start(); err != nil {
		b.Fatalf("server start: %v", err)
	}
	defer svr.Stop()

	cli := NewClient(addr)
	if err := cli.Connect(); err != nil {
		b.Fatalf("client connect: %v", err)
	}
	defer cli.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var out pbbase.String
		if err := cli.InvokeProto(ctx, "pbbase.String/echo_bench",
			&pbbase.String{Value: "bench"},
			&out,
		); err != nil {
			b.Fatalf("invoke proto: %v", err)
		}
	}
}

// BenchmarkGNRPC_InvokeProto_Parallel measures proto RPC throughput under parallel clients.
func BenchmarkGNRPC_InvokeProto_Parallel(b *testing.B) {
	port, err := xnet.AssignRandPort("127.0.0.1")
	if err != nil {
		b.Fatalf("assign port: %v", err)
	}
	addr := "127.0.0.1:" + strconv.Itoa(port)

	svr := NewServer(addr)
	svr.RegisterProto("pbbase.String/echo_bench",
		func() proto.Message { return &pbbase.String{} },
		func(ctx context.Context, req proto.Message) (proto.Message, error) {
			_ = ctx
			in := req.(*pbbase.String)
			return &pbbase.String{Value: in.Value}, nil
		},
	)
	if err := svr.Start(); err != nil {
		b.Fatalf("server start: %v", err)
	}
	defer svr.Stop()

	cli := NewClient(addr)
	if err := cli.Connect(); err != nil {
		b.Fatalf("client connect: %v", err)
	}
	defer cli.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var out pbbase.String
			if err := cli.InvokeProto(ctx, "pbbase.String/echo_bench",
				&pbbase.String{Value: "bench"},
				&out,
			); err != nil {
				b.Fatalf("invoke proto: %v", err)
			}
		}
	})
}

