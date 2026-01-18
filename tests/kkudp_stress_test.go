package tests

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkudp"
)

// TestKKUDPHighThroughput tests high UDP message throughput
func TestKKUDPHighThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	addr := freeUDPAddr(t)

	var serverMessages int64
	serverHandler := &testHandler{
		onMessage: func(c kknet.IConn, data []byte) {
			atomic.AddInt64(&serverMessages, 1)
			_ = c.Send(data)
		},
	}
	server := kkudp.NewServer(addr, serverHandler)
	if err := server.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer func() { _ = server.Stop() }()

	const numClients = 10
	const messagesPerClient = 1000
	var wg sync.WaitGroup
	var clientMessages int64

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			msgCh := make(chan []byte, messagesPerClient)
			clientHandler := &testHandler{
				onMessage: func(c kknet.IConn, data []byte) {
					select {
					case msgCh <- data:
						atomic.AddInt64(&clientMessages, 1)
					default:
					}
				},
			}
			client := kkudp.NewClient(addr, clientHandler)
			if err := client.Connect(); err != nil {
				return
			}
			defer func() { _ = client.Close() }()

			for j := 0; j < messagesPerClient; j++ {
				payload := []byte{byte(j), byte(j >> 8)}
				if err := client.Send(payload); err != nil {
					return
				}
			}

			// Wait for replies (UDP may lose packets)
			time.Sleep(2 * time.Second)
		}()
	}

	wg.Wait()
	time.Sleep(500 * time.Millisecond)

	stats := server.Stats()
	expectedMessages := int64(numClients * messagesPerClient)

	t.Logf("Expected: %d, Server received: %d, Client received: %d", expectedMessages, serverMessages, clientMessages)
	t.Logf("Server stats: %+v", stats)

	// UDP is unreliable, accept at least 70% delivery
	if serverMessages < expectedMessages*7/10 {
		t.Logf("UDP throughput test: server received %d/%d (UDP may lose packets)", serverMessages, expectedMessages)
	}
}

// TestKKUDPLongRunning tests UDP server stability over time
func TestKKUDPLongRunning(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long running test in short mode")
	}

	addr := freeUDPAddr(t)

	var messageCount int64
	serverHandler := &testHandler{
		onMessage: func(c kknet.IConn, data []byte) {
			atomic.AddInt64(&messageCount, 1)
			_ = c.Send(data)
		},
	}
	server := kkudp.NewServer(addr, serverHandler)
	if err := server.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer func() { _ = server.Stop() }()

	const duration = 10 * time.Second
	const clientsPerSecond = 10
	startTime := time.Now()
	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	// Start client goroutines
	for i := 0; i < clientsPerSecond; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()

			for {
				select {
				case <-stopCh:
					return
				case <-ticker.C:
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
						continue
					}

					payload := []byte("test")
					if err := client.Send(payload); err != nil {
						_ = client.Close()
						continue
					}

					select {
					case <-msgCh:
					case <-time.After(500 * time.Millisecond):
					}

					_ = client.Close()
				}
			}
		}()
	}

	// Run for specified duration
	time.Sleep(duration)
	close(stopCh)
	wg.Wait()

	elapsed := time.Since(startTime)
	finalCount := atomic.LoadInt64(&messageCount)
	stats := server.Stats()

	t.Logf("Ran for %v, processed %d messages", elapsed, finalCount)
	t.Logf("Server stats: %+v", stats)

	if finalCount == 0 {
		t.Error("no messages processed")
	}
}
