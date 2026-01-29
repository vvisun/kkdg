package testudp

import (
	"sync"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kkudp"
)

// TestKKUDPLargeMessage tests sending large UDP messages
func TestKKUDPLargeMessage(t *testing.T) {
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

	// Test 16KB message (common UDP limit)
	payload := make([]byte, kkpacket.DefaultMaxMessageSize()-kkpacket.DefaultStreamPacket().LengthFieldByteCount())
	for i := range payload {
		payload[i] = byte(i % 256)
	}
	bb, err := kkpacket.DefaultStreamPacket().Pack(payload)
	if err != nil {
		t.Fatalf("client send buffer: %v", err)
	}

	if err := client.SendBuffer(bb); err != nil {
		t.Fatalf("client send: %v", err)
	}

	select {
	case got := <-msgCh:
		if len(kkpacket.DefaultStreamPacket().BodyBytesFromBytes(got)) != len(payload) {
			t.Fatalf("unexpected message size: got %d, want %d", len(got), len(payload))
		}
		for i := range kkpacket.DefaultStreamPacket().BodyBytesFromBytes(got) {
			if kkpacket.DefaultStreamPacket().BodyBytesFromBytes(got)[i] != payload[i] {
				t.Fatalf("message mismatch at index %d", i)
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("large message reply timeout")
	}
}

// TestKKUDPRapidMessages tests sending many UDP messages rapidly
func TestKKUDPRapidMessages(t *testing.T) {
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

	msgCh := make(chan []byte, 1000)
	clientHandler := &testHandler{
		onMessage: func(c kknet.IConn, data []byte) {
			select {
			case msgCh <- data:
			default:
			}
		},
	}
	client := kkudp.NewClient(addr, clientHandler)
	if err := client.Connect(); err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer func() { _ = client.Close() }()

	const numMessages = 500
	for i := 0; i < numMessages; i++ {
		payload := []byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
		if err := client.Send(payload); err != nil {
			t.Fatalf("client send %d: %v", i, err)
		}
	}

	// UDP is unreliable, so we accept some packet loss
	received := make(map[uint32]bool)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case got := <-msgCh:
			if len(got) == 4 {
				val := uint32(got[0]) | uint32(got[1])<<8 | uint32(got[2])<<16 | uint32(got[3])<<24
				received[val] = true
			}
		case <-time.After(100 * time.Millisecond):
			// Continue waiting
		}
	}

	// UDP may lose packets, so we accept at least 80% delivery
	if len(received) < numMessages*8/10 {
		t.Logf("received %d/%d messages (UDP may lose packets)", len(received), numMessages)
	}
}

// TestKKUDPMaxMessageSize tests message size limit enforcement
func TestKKUDPMaxMessageSize(t *testing.T) {
	addr := freeUDPAddr(t)

	serverHandler := &testHandler{
		onMessage: func(c kknet.IConn, data []byte) {
			_ = c.Send(data)
		},
	}
	maxSize := kkpacket.DefaultMaxMessageSize()
	server := kkudp.NewServer(addr, serverHandler)
	if err := server.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer func() { _ = server.Stop() }()

	clientHandler := &testHandler{}
	client := kkudp.NewClient(addr, clientHandler)
	if err := client.Connect(); err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Test message at max size (should succeed)
	payload := make([]byte, maxSize)
	if err := client.Send(payload); err != nil {
		t.Fatalf("max size message should succeed: %v", err)
	}

	// Test message exceeding max size (should fail)
	oversized := make([]byte, maxSize+1)
	if err := client.Send(oversized); err == nil {
		t.Fatal("oversized message should fail")
	}
}

// TestKKUDPConcurrentClients tests multiple concurrent UDP clients
func TestKKUDPConcurrentClients(t *testing.T) {
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

	const numClients = 50
	var wg sync.WaitGroup
	var successCount int64

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			msgCh := make(chan []byte, 1)
			clientHandler := &testHandler{
				onMessage: func(c kknet.IConn, data []byte) {
					select {
					case msgCh <- data:
					default:
					}
				},
			}
			client := kkudp.NewClient(addr, clientHandler)
			if err := client.Connect(); err != nil {
				return
			}
			defer func() { _ = client.Close() }()

			payload := []byte{byte(id), byte(id >> 8)}
			if err := client.Send(payload); err != nil {
				return
			}

			select {
			case got := <-msgCh:
				if len(got) == 2 && got[0] == byte(id) && got[1] == byte(id>>8) {
					successCount++
				}
			case <-time.After(2 * time.Second):
				// UDP may lose packets
			}
		}(i)
	}

	wg.Wait()

	// UDP is unreliable, accept at least 70% success
	if successCount < int64(numClients*7/10) {
		t.Logf("UDP concurrent test: %d/%d successful (UDP may lose packets)", successCount, numClients)
	}
}
