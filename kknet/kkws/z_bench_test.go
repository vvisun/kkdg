package kkws

import (
	"net"
	"testing"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
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
	srv := NewServer(addr, nil, kknet.DefaultOptions())
	if err := srv.Start(); err != nil {
		b.Fatalf("Start: %v", err)
	}
	defer srv.Stop()

	client := NewClient("ws://"+addr+"/ws", nil, kknet.DefaultOptions())
	if err := client.Connect(); err != nil {
		b.Fatalf("Connect: %v", err)
	}
	defer client.Close()

	payload := []byte("bench")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		bb, err := kkpacket.DefaultStreamPacket().Pack(payload)
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
	srv := NewServer(addr, nil, kknet.DefaultOptions())
	if err := srv.Start(); err != nil {
		b.Fatalf("Start: %v", err)
	}
	defer srv.Stop()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		client := NewClient("ws://"+addr+"/ws", nil, kknet.DefaultOptions())
		_ = client.Connect()
		_ = client.Close()
	}
}
