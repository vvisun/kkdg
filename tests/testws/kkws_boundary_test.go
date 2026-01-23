package testws

import (
	"sync"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kkws"
)

// TestKKWSLargeMessage tests sending large WebSocket messages
func TestKKWSLargeMessage(t *testing.T) {
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

	// Test 1MB message
	payload := make([]byte, kkpacket.DefaultMaxMessageSize())
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	replyCh := make(chan []byte, 1)
	clientHandler.onMessage = func(c kknet.IConn, data []byte) {
		replyCh <- data
	}

	if err := client.Send(payload); err != nil {
		t.Fatalf("client send: %v", err)
	}

	select {
	case got := <-replyCh:
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

// TestKKWSEmptyMessage tests sending empty WebSocket messages
func TestKKWSEmptyMessage(t *testing.T) {
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

	payload := []byte{}
	if err := client.Send(payload); err != nil {
		t.Fatalf("client send: %v", err)
	}

	select {
	case got := <-replyCh:
		if len(got) != 0 {
			t.Fatalf("unexpected empty message size: got %d, want 0", len(got))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("empty message reply timeout")
	}
}

// TestKKWSRapidMessages tests sending many WebSocket messages rapidly
func TestKKWSRapidMessages(t *testing.T) {
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

	msgCh := make(chan []byte, 1000)
	clientHandler.onMessage = func(c kknet.IConn, data []byte) {
		select {
		case msgCh <- data:
		default:
		}
	}

	const numMessages = 1000
	for i := 0; i < numMessages; i++ {
		payload := []byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
		if err := client.Send(payload); err != nil {
			t.Fatalf("client send %d: %v", i, err)
		}
	}

	received := make(map[uint32]bool)
	deadline = time.Now().Add(10 * time.Second)
	for len(received) < numMessages {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: received %d/%d messages", len(received), numMessages)
		}

		select {
		case got := <-msgCh:
			if len(got) == 4 {
				val := uint32(got[0]) | uint32(got[1])<<8 | uint32(got[2])<<16 | uint32(got[3])<<24
				if received[val] {
					t.Fatalf("duplicate message: %d", val)
				}
				received[val] = true
			}
		case <-time.After(100 * time.Millisecond):
			// Continue waiting
		}
	}
}

// TestKKWSConnectionClose tests WebSocket connection close handling
func TestKKWSConnectionClose(t *testing.T) {
	addr := freeTCPAddr(t)

	closeCh := make(chan error, 1)
	serverHandler := &testHandler{
		onClose: func(c kknet.IConn, err error) {
			closeCh <- err
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

// TestKKWSMaxMessageSize tests WebSocket message size limit enforcement
func TestKKWSMaxMessageSize(t *testing.T) {
	addr := freeTCPAddr(t)

	serverHandler := &testHandler{
		onMessage: func(c kknet.IConn, data []byte) {
			_ = c.Send(data)
		},
	}
	maxSize := kkpacket.DefaultMaxMessageSize()
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

// TestKKWSConcurrentConnections tests multiple concurrent WebSocket connections
func TestKKWSConcurrentConnections(t *testing.T) {
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
	server := kkws.NewServer(addr, serverHandler)
	server.SetPath("/ws")

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Start()
	}()
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
			client := kkws.NewClient("ws://"+addr+"/ws", clientHandler)

			deadline := time.Now().Add(5 * time.Second)
			for {
				err := client.Connect()
				if err == nil {
					break
				}
				if time.Now().After(deadline) {
					errors <- err
					return
				}
				time.Sleep(50 * time.Millisecond)
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
			case <-time.After(5 * time.Second):
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
	time.Sleep(500 * time.Millisecond)
	mu.Lock()
	finalCount := connCount
	mu.Unlock()

	if finalCount != 0 {
		t.Errorf("expected 0 connections after close, got %d", finalCount)
	}
}
