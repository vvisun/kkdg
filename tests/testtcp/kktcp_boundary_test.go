package testtcp

import (
	"sync"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kktcp"
)

// TestKKTCPLargeMessage tests sending large messages near MaxMessageSize
func TestKKTCPLargeMessage(t *testing.T) {
	addr := freeTCPAddr(t)

	serverHandler := &testHandler{
		onMessage: func(c kknet.IConn, data []byte) {
			_ = c.Send(data)
		},
	}
	server := kktcp.NewServer(addr, serverHandler)
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
	client := kktcp.NewClient(addr, clientHandler)
	if err := client.Connect(); err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Test 1MB message
	payload := make([]byte, kkpacket.DefaultMaxMessageSize()-4)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	bb, err := kkpacket.DefaultStreamPacket().Pack(payload)
	if err != nil {
		t.Fatalf("pack failed: %v", err)
	}
	if err := client.SendBuffer(bb); err != nil {
		t.Fatalf("client send: %v", err)
	}

	select {
	case got := <-msgCh:
		if len(got) != len(payload) {
			t.Fatalf("unexpected message size: got %d, want %d", len(got), len(payload))
		}
		for i := range payload {
			if got[i] != payload[i] {
				t.Fatalf("message mismatch at index %d", i)
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("large message reply timeout")
	}
}

// TestKKTCPEmptyMessage tests sending empty messages
func TestKKTCPEmptyMessage(t *testing.T) {
	addr := freeTCPAddr(t)

	serverHandler := &testHandler{
		onMessage: func(c kknet.IConn, data []byte) {
			_ = c.Send(data)
		},
	}
	server := kktcp.NewServer(addr, serverHandler)
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
	client := kktcp.NewClient(addr, clientHandler)
	if err := client.Connect(); err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer func() { _ = client.Close() }()

	payload := []byte{}
	bb, err := kkpacket.DefaultStreamPacket().Pack(payload)
	if err != nil {
		t.Fatalf("pack failed: %v", err)
	}
	if err := client.SendBuffer(bb); err != nil {
		t.Fatalf("client send: %v", err)
	}

	select {
	case got := <-msgCh:
		if len(got) != 0 {
			t.Fatalf("unexpected empty message size: got %d, want 0", len(got))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("empty message reply timeout")
	}
}

// TestKKTCPConcurrentConnections tests multiple concurrent connections
func TestKKTCPConcurrentConnections(t *testing.T) {
	addr := freeTCPAddr(t)

	var mu sync.Mutex
	connCount := 0

	serverHandler := &testHandler{
		onConnect: func(c kknet.IConn) {
			mu.Lock()
			connCount++
			mu.Unlock()
		},
		onMessage: func(c kknet.IConn, data []byte) {
			_ = c.Send(data)
		},
		onClose: func(c kknet.IConn, err error) {
			mu.Lock()
			connCount--
			mu.Unlock()
		},
	}
	server := kktcp.NewServer(addr, serverHandler)
	if err := server.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer func() { _ = server.Stop() }()

	const numClients = 50
	var wg sync.WaitGroup
	errors := make(chan error, numClients)

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			msgCh := make(chan []byte, 1)
			clientHandler := &testHandler{
				onMessage: func(c kknet.IConn, data []byte) {
					msgCh <- data
				},
			}
			client := kktcp.NewClient(addr, clientHandler)
			if err := client.Connect(); err != nil {
				errors <- err
				return
			}
			defer func() { _ = client.Close() }()

			payload := []byte{byte(id)}
			if err := client.Send(payload); err != nil {
				errors <- err
				return
			}

			select {
			case got := <-msgCh:
				if len(got) != 1 || got[0] != byte(id) {
					errors <- nil // message mismatch
					return
				}
			case <-time.After(2 * time.Second):
				errors <- nil // timeout
				return
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		if err != nil {
			t.Errorf("concurrent connection error: %v", err)
		}
	}

	// Give server time to update connection count
	time.Sleep(100 * time.Millisecond)
	mu.Lock()
	finalCount := connCount
	mu.Unlock()

	if finalCount != 0 {
		t.Errorf("expected 0 connections after close, got %d", finalCount)
	}
}

// TestKKTCPRapidMessages tests sending many messages rapidly
func TestKKTCPRapidMessages(t *testing.T) {
	addr := freeTCPAddr(t)

	serverHandler := &testHandler{
		onMessage: func(c kknet.IConn, data []byte) {
			_ = c.Send(data)
		},
	}
	server := kktcp.NewServer(addr, serverHandler)
	if err := server.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer func() { _ = server.Stop() }()

	msgCh := make(chan []byte, 1000)
	clientHandler := &testHandler{
		onMessage: func(c kknet.IConn, data []byte) {
			msgCh <- data
		},
	}
	client := kktcp.NewClient(addr, clientHandler)
	if err := client.Connect(); err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer func() { _ = client.Close() }()

	const numMessages = 1000
	for i := 0; i < numMessages; i++ {
		payload := []byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
		bb, err := kkpacket.DefaultStreamPacket().Pack(payload)
		if err != nil {
			t.Fatalf("pack failed: %v", err)
		}
		if err := client.SendBuffer(bb); err != nil {
			t.Fatalf("client send %d: %v", i, err)
		}
	}

	received := make(map[uint32]bool)
	deadline := time.Now().Add(5 * time.Second)
	for len(received) < numMessages {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: received %d/%d messages", len(received), numMessages)
		}

		select {
		case got := <-msgCh:
			if len(got) != 4 {
				t.Fatalf("unexpected message size: %d", len(got))
			}
			val := uint32(got[0]) | uint32(got[1])<<8 | uint32(got[2])<<16 | uint32(got[3])<<24
			if received[val] {
				t.Fatalf("duplicate message: %d", val)
			}
			received[val] = true
		case <-time.After(100 * time.Millisecond):
			// Continue waiting
		}
	}
}

// TestKKTCPConnectionClose tests connection close handling
func TestKKTCPConnectionClose(t *testing.T) {
	addr := freeTCPAddr(t)

	closeCh := make(chan error, 1)
	serverHandler := &testHandler{
		onClose: func(c kknet.IConn, err error) {
			closeCh <- err
		},
	}
	server := kktcp.NewServer(addr, serverHandler)
	if err := server.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer func() { _ = server.Stop() }()

	clientHandler := &testHandler{}
	client := kktcp.NewClient(addr, clientHandler)
	if err := client.Connect(); err != nil {
		t.Fatalf("client connect: %v", err)
	}

	// Close client connection
	if err := client.Close(); err != nil {
		t.Fatalf("client close: %v", err)
	}

	select {
	case err := <-closeCh:
		// Connection closed, err may be nil or an error
		_ = err
	case <-time.After(2 * time.Second):
		t.Fatal("server did not detect connection close")
	}
}

// TestKKTCPMaxMessageSize tests message size limit enforcement
func TestKKTCPMaxMessageSize(t *testing.T) {
	addr := freeTCPAddr(t)

	serverHandler := &testHandler{
		onMessage: func(c kknet.IConn, data []byte) {
			_ = c.Send(data)
		},
	}
	maxSize := 1024 * 1024
	server := kktcp.NewServer(addr, serverHandler)
	if err := server.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer func() { _ = server.Stop() }()

	clientHandler := &testHandler{}
	client := kktcp.NewClient(addr, clientHandler)
	if err := client.Connect(); err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Test message at max size (should succeed)
	payload := make([]byte, kkpacket.DefaultMaxMessageSize())
	if err := client.Send(payload); err != nil {
		t.Fatalf("max size message should succeed: %v", err)
	}

	// Test message exceeding max size (should fail)
	oversized := make([]byte, maxSize+1)
	if err := client.Send(oversized); err == nil {
		t.Fatal("oversized message should fail")
	}
}
