package tests

import (
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkws"
)

func TestKKWSRoundtrip(t *testing.T) {
	addr := freeTCPAddr(t)

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
			t.Fatalf("client connect: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	defer func() { _ = client.Close() }()

	replyCh := make(chan []byte, 1)
	clientHandler.onMessage = func(c kknet.IConn, data []byte) {
		replyCh <- data
	}

	payload := []byte("ping")
	if err := client.Send(payload); err != nil {
		t.Fatalf("client send: %v", err)
	}

	select {
	case got := <-replyCh:
		if string(got) != string(payload) {
			t.Fatalf("unexpected ws reply: %s", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ws reply timeout")
	}

	srvStats := server.Stats()
	if srvStats.RecvMsgs == 0 || srvStats.SentMsgs == 0 {
		t.Fatalf("server stats not updated: %+v", srvStats)
	}
	cliStats := client.Stats()
	if cliStats.RecvMsgs == 0 || cliStats.SentMsgs == 0 {
		t.Fatalf("client stats not updated: %+v", cliStats)
	}
}
