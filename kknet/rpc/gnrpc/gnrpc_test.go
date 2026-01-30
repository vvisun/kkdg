package gnrpc

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/vvisun/kkdg/utils/xnet"
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

