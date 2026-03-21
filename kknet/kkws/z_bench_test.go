package kkws

import (
	"net"
	"testing"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/kklog"
)

func freePortBench(b *testing.B) string {
	b.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func BenchmarkWSConn_SendBuffer(b *testing.B) {
	addr := freePortBench(b)
	opts := kknet.ApplyOptions(
		kknet.WithRawHandler(&noopRawHandler{}),
		kknet.WithLogger(kklog.Nop()),
	)
	srv := NewServer(addr, nil, opts)
	if err := srv.Start(); err != nil {
		b.Fatalf("Start: %v", err)
	}
	defer srv.Stop()

	client := NewClient("ws://"+addr+"/ws", nil, opts)
	if err := client.Connect(); err != nil {
		b.Fatalf("Connect: %v", err)
	}
	defer client.Close()

	payload := make([]byte, 512)
	for i := range payload {
		payload[i] = 0x01
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		bb, err := opts.StreamTool.Pack(payload)
		if err != nil {
			b.Fatal(err)
		}
		if err := client.SendBuffer(bb); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkServer_AcceptAndClose(b *testing.B) {
	addr := freePortBench(b)
	opts := kknet.ApplyOptions(
		kknet.WithRawHandler(&noopRawHandler{}),
		kknet.WithLogger(kklog.Nop()),
	)
	srv := NewServer(addr, nil, opts)
	if err := srv.Start(); err != nil {
		b.Fatalf("Start: %v", err)
	}
	defer srv.Stop()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		client := NewClient("ws://"+addr+"/ws", nil, opts)
		_ = client.Connect()
		_ = client.Close()
	}
}
