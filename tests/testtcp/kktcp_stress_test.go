package testtcp

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kktcp"
)

// TestKKTCPHighConcurrency tests server with many concurrent connections
func TestKKTCPHighConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	addr := freeTCPAddr(t)

	var activeConns int64
	serverHandler := &testHandler{
		onConnect: func(c kknet.IConn) {
			atomic.AddInt64(&activeConns, 1)
		},
		onMessage: func(c kknet.IConn, data []byte) {
			_ = c.Send(data)
		},
		onClose: func(c kknet.IConn, err error) {
			atomic.AddInt64(&activeConns, -1)
		},
	}
	server := kktcp.NewServer(addr, serverHandler, kknet.WithPoolSize(200))
	if err := server.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer func() { _ = server.Stop() }()

	const numClients = 5000
	const batchSize = 50
	var wg sync.WaitGroup
	var successCount int64
	var errorCount int64

	// Process clients in batches to avoid overwhelming the server
	for batch := 0; batch < numClients/batchSize; batch++ {
		for i := 0; i < batchSize; i++ {
			id := batch*batchSize + i
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
				client := kktcp.NewClient(addr, clientHandler)

				// Retry connection if it fails
				var err error
				for retry := 0; retry < 3; retry++ {
					err = client.Connect()
					if err == nil {
						break
					}
					time.Sleep(10 * time.Millisecond)
				}
				if err != nil {
					atomic.AddInt64(&errorCount, 1)
					return
				}
				defer func() { _ = client.Close() }()

				payload := []byte{byte(id), byte(id >> 8)}
				bb, err := kkpacket.DefaultStreamPacket().Pack(payload)
				if err != nil {
					atomic.AddInt64(&errorCount, 1)
					return
				}
				if err := client.SendBuffer(bb); err != nil {
					atomic.AddInt64(&errorCount, 1)
					return
				}

				select {
				case got := <-msgCh:
					if len(got) == 2 && got[0] == byte(id) && got[1] == byte(id>>8) {
						atomic.AddInt64(&successCount, 1)
					} else {
						atomic.AddInt64(&errorCount, 1)
					}
				case <-time.After(10 * time.Second):
					atomic.AddInt64(&errorCount, 1)
				}
			}(id)
		}
		// Small delay between batches to avoid overwhelming
		if batch < numClients/batchSize-1 {
			time.Sleep(50 * time.Millisecond)
		}
	}

	wg.Wait()

	// Give server time to update stats and close connections
	time.Sleep(2 * time.Second)

	stats := server.Stats()
	t.Logf("Server stats: %+v", stats)
	t.Logf("Success: %d, Errors: %d, Active connections: %d", successCount, errorCount, atomic.LoadInt64(&activeConns))

	fmt.Printf("Server stats: %+v\n", stats)
	fmt.Printf("Success: %d, Errors: %d, Active connections: %d\n", successCount, errorCount, atomic.LoadInt64(&activeConns))

	// Accept 80% success rate for high concurrency test
	if successCount < int64(numClients*8/10) {
		t.Errorf("too many failures: success %d/%d", successCount, numClients)
	}

	// Allow a few connections to be in closing state
	if atomic.LoadInt64(&activeConns) > 5 {
		t.Errorf("too many active connections: %d", atomic.LoadInt64(&activeConns))
	}
}

// TestKKTCPLongRunning tests server stability over time
func TestKKTCPLongRunning(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long running test in short mode")
	}

	addr := freeTCPAddr(t)

	var messageCount int64
	serverHandler := &testHandler{
		onMessage: func(c kknet.IConn, data []byte) {
			atomic.AddInt64(&messageCount, 1)
			_ = c.Send(data)
		},
	}
	server := kktcp.NewServer(addr, serverHandler, kknet.WithPoolSize(50))
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
					client := kktcp.NewClient(addr, clientHandler)
					if err := client.Connect(); err != nil {
						continue
					}

					payload := []byte("test")
					bb, err := kkpacket.DefaultStreamPacket().Pack(payload)
					if err != nil {
						_ = client.Close()
						continue
					}
					if err := client.SendBuffer(bb); err != nil {
						_ = client.Close()
						continue
					}

					select {
					case <-msgCh:
					case <-time.After(1 * time.Second):
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

// TestKKTCPHighThroughput tests high message throughput
func TestKKTCPHighThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping throughput test in short mode")
	}

	addr := freeTCPAddr(t)

	var serverMessages int64
	serverHandler := &testHandler{
		onMessage: func(c kknet.IConn, data []byte) {
			atomic.AddInt64(&serverMessages, 1)
			_ = c.Send(data)
		},
	}
	server := kktcp.NewServer(addr, serverHandler, kknet.WithPoolSize(100))
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
			client := kktcp.NewClient(addr, clientHandler)
			if err := client.Connect(); err != nil {
				return
			}
			defer func() { _ = client.Close() }()

			for j := 0; j < messagesPerClient; j++ {
				payload := []byte{byte(j), byte(j >> 8)}
				bb, err := kkpacket.DefaultStreamPacket().Pack(payload)
				if err != nil {
					return
				}
				if err := client.SendBuffer(bb); err != nil {
					return
				}
			}

			// Wait for all replies
			deadline := time.Now().Add(10 * time.Second)
			for len(msgCh) < messagesPerClient {
				if time.Now().After(deadline) {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}

	wg.Wait()

	time.Sleep(500 * time.Millisecond)
	stats := server.Stats()

	expectedMessages := int64(numClients * messagesPerClient)
	t.Logf("Expected: %d, Server received: %d, Client received: %d", expectedMessages, serverMessages, clientMessages)
	t.Logf("Server stats: %+v", stats)

	if serverMessages < expectedMessages*9/10 {
		t.Errorf("server received too few messages: %d/%d", serverMessages, expectedMessages)
	}

	if clientMessages < expectedMessages*9/10 {
		t.Errorf("clients received too few messages: %d/%d", clientMessages, expectedMessages)
	}
}

// TestKKTCPMemoryLeak tests for memory leaks with connection churn
func TestKKTCPMemoryLeak(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping memory leak test in short mode")
	}

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

	const iterations = 1000
	const connectionsPerIteration = 10

	for i := 0; i < iterations; i++ {
		var wg sync.WaitGroup
		for j := 0; j < connectionsPerIteration; j++ {
			wg.Add(1)
			go func() {
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
				client := kktcp.NewClient(addr, clientHandler)
				if err := client.Connect(); err != nil {
					return
				}

				payload := []byte("test")
				bb, err := kkpacket.DefaultStreamPacket().Pack(payload)
				if err != nil {
					_ = client.Close()
					return
				}
				if err := client.SendBuffer(bb); err != nil {
					_ = client.Close()
					return
				}

				select {
				case <-msgCh:
				case <-time.After(1 * time.Second):
				}

				_ = client.Close()
			}()
		}
		wg.Wait()

		if i%100 == 0 {
			stats := server.Stats()
			t.Logf("Iteration %d: stats %+v", i, stats)
		}
	}

	// Wait for all connections to close (gnet may need time to process closes)
	deadline := time.Now().Add(10 * time.Second)
	for {
		stats := server.Stats()
		if stats.ActiveConns == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Logf("Final stats: %+v", stats)
			// Allow a few connections in closing state (race condition with gnet)
			// This is acceptable as connections may be in the process of closing
			if stats.ActiveConns > 5 {
				t.Errorf("too many active connections after wait: %d (expected 0-5)", stats.ActiveConns)
			} else {
				t.Logf("%d connection(s) still active (may be in closing state, acceptable)", stats.ActiveConns)
			}
			return
		}
		time.Sleep(200 * time.Millisecond)
	}

	stats := server.Stats()
	t.Logf("Final stats: %+v", stats)

	// Allow a small number of connections in closing state
	if stats.ActiveConns > 5 {
		t.Errorf("expected 0-5 active connections, got %d", stats.ActiveConns)
	}
}
