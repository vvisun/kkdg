package kktcp

import (
	"net"
	"testing"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
)

// freePortBench returns a free TCP addr for benchmarks.
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

// BenchmarkTCPConn_SendBuffer mirrors kkws.BenchmarkWSConn_SendBuffer, measuring
// end-to-end SendBuffer throughput for a single TCP client<->server pair.
func BenchmarkTCPConn_SendBuffer(b *testing.B) {
	addr := freePortBench(b)
	opts := kknet.ApplyOptions(kknet.WithRawHandler(&noopRawHandler{}))
	srv := NewServer(addr, nil, opts)
	if err := srv.Start(); err != nil {
		b.Fatalf("Start: %v", err)
	}
	defer srv.Stop()

	client := NewClient(addr, nil, opts)
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
		bb, err := kkpacket.DefaultStreamPacket().Pack(payload)
		if err != nil {
			b.Fatal(err)
		}
		if err := client.SendBuffer(bb); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTCPServer_AcceptAndClose mirrors kkws.BenchmarkServer_AcceptAndClose,
// focusing on connection lifecycle cost on the server side.
func BenchmarkTCPServer_AcceptAndClose(b *testing.B) {
	addr := freePortBench(b)
	opts := kknet.ApplyOptions(kknet.WithRawHandler(&noopRawHandler{}))
	srv := NewServer(addr, nil, opts)
	if err := srv.Start(); err != nil {
		b.Fatalf("Start: %v", err)
	}
	defer srv.Stop()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		client := NewClient(addr, nil, opts)
		_ = client.Connect()
		_ = client.Close()
	}
}

