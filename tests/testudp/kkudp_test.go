package testudp

import (
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kkudp"
)

func TestKKNetUDP(t *testing.T) {
	addr := freeUDPAddr(t)

	serverHandler := &testHandler{
		onMessage: func(c kknet.IConn, data []byte) {
			_ = c.Send(data)
		},
	}
	server := kkudp.NewServer(addr, serverHandler)
	if err := server.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer func() { _ = server.Stop() }()

	msgCh := make(chan []byte, 1)
	clientHandler := &testHandler{
		onMessage: func(c kknet.IConn, data []byte) {
			msgCh <- data
		},
	}
	client := kkudp.NewClient(addr, clientHandler)
	if err := client.Connect(); err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer func() { _ = client.Close() }()

	payload := []byte("ping")
	bb, err := kkpacket.DefaultStreamPacket().Pack(payload)
	if err != nil {
		t.Fatalf("client send buffer: %v", err)
	}
	if err := client.SendBuffer(bb); err != nil {
		t.Fatalf("client send: %v", err)
	}

	select {
	case got := <-msgCh:
		if string(kkpacket.DefaultStreamPacket().BodyBytesFromBytes(got)) != "ping" {
			t.Fatalf("unexpected udp reply: %s, expected: %s", string(kkpacket.DefaultStreamPacket().BodyBytesFromBytes(got)), "ping")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("udp reply timeout")
	}
}
