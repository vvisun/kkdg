package testtcp

import (
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kktcp"
)

func BenchmarkKKNetTCPRoundtrip(b *testing.B) {
	addr := freeTCPAddr(b)

	serverHandler := &testHandler{
		onMessage: func(c kknet.IConn, data []byte) {
			_ = c.Send(data)
		},
	}
	server := kktcp.NewServer(addr, serverHandler)
	if err := server.Start(); err != nil {
		b.Fatalf("server start: %v", err)
	}
	defer func() { _ = server.Stop() }()

	msgCh := make(chan []byte, 1024)
	clientHandler := &testHandler{
		onMessage: func(c kknet.IConn, data []byte) {
			msgCh <- data
		},
	}
	client := kktcp.NewClient(addr, clientHandler)
	if err := client.Connect(); err != nil {
		b.Fatalf("client connect: %v", err)
	}
	defer func() { _ = client.Close() }()

	payload := []byte("ping")
	bb, err := kkpacket.DefaultStreamPacket().Pack(payload, kkpacket.DefaultMaxMessageSize())
	if err != nil {
		b.Fatalf("pack failed: %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bb, _ = kkpacket.DefaultStreamPacket().Pack(payload, kkpacket.DefaultMaxMessageSize())
		if err := client.SendBuffer(bb); err != nil {
			b.Fatalf("client send: %v", err)
		}
		select {
		case <-msgCh:
		case <-time.After(1 * time.Second):
			b.Fatal("tcp reply timeout")
		}
	}
}
