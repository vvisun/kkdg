package tests

import (
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkws"
)

func BenchmarkKKWSRoundtrip(b *testing.B) {
	addr := freeTCPAddr(b)

	serverHandler := &testHandler{
		onMessage: func(c kknet.IConn, data []byte) {
			_ = c.Send(data)
		},
	}
	server := kkws.NewServer(addr, serverHandler)
	server.SetPath("/ws")

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Start()
	}()
	defer func() { _ = server.Stop() }()

	clientHandler := &testHandler{}
	client := kkws.NewClient("ws://"+addr+"/ws", clientHandler)

	deadline := time.Now().Add(2 * time.Second)
	for {
		err := client.Connect()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			b.Fatalf("client connect: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	defer func() { _ = client.Close() }()

	replyCh := make(chan []byte, 1)
	clientHandler.onMessage = func(c kknet.IConn, data []byte) {
		replyCh <- data
	}

	payload := []byte("ping")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := client.Send(payload); err != nil {
			b.Fatalf("client send: %v", err)
		}
		select {
		case <-replyCh:
		case <-time.After(2 * time.Second):
			b.Fatal("ws reply timeout")
		}
	}
}
