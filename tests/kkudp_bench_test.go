package tests

import (
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkudp"
)

func BenchmarkKKNetUDPRoundtrip(b *testing.B) {
	addr := freeUDPAddr(b)

	serverHandler := &testHandler{
		onMessage: func(c kknet.Conn, data []byte) {
			_ = c.Send(data)
		},
	}
	server := kkudp.NewServer(addr, serverHandler)
	if err := server.Start(); err != nil {
		b.Fatalf("server start: %v", err)
	}
	defer func() { _ = server.Stop() }()

	msgCh := make(chan []byte, 1)
	clientHandler := &testHandler{
		onMessage: func(c kknet.Conn, data []byte) {
			msgCh <- data
		},
	}
	client := kkudp.NewClient(addr, clientHandler)
	if err := client.Connect(); err != nil {
		b.Fatalf("client connect: %v", err)
	}
	defer func() { _ = client.Close() }()

	payload := []byte("ping")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := client.Send(payload); err != nil {
			b.Fatalf("client send: %v", err)
		}
		select {
		case <-msgCh:
		case <-time.After(2 * time.Second):
			b.Fatal("udp reply timeout")
		}
	}
}
