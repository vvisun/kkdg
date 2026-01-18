package tests

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkws"
)

// TestKKWSHighConcurrency tests WebSocket server with many concurrent connections
func TestKKWSHighConcurrency(t *testing.T) {
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
	server := kkws.NewServer(addr, serverHandler, kknet.WithPoolSize(100))
	server.SetPath("/ws")

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Start()
	}()
	defer func() { _ = server.Stop() }()

	const numClients = 500
	var wg sync.WaitGroup
	var successCount int64
	var errorCount int64

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
			client := kkws.NewClient("ws://"+addr+"/ws", clientHandler)

			deadline := time.Now().Add(10 * time.Second)
			for {
				err := client.Connect()
				if err == nil {
					break
				}
				if time.Now().After(deadline) {
					atomic.AddInt64(&errorCount, 1)
					return
				}
				time.Sleep(50 * time.Millisecond)
			}
			defer func() { _ = client.Close() }()

			payload := []byte{byte(id), byte(id >> 8)}
			if err := client.Send(payload); err != nil {
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
			case <-time.After(5 * time.Second):
				atomic.AddInt64(&errorCount, 1)
			}
		}(i)
	}

	wg.Wait()

	// Give server time to update stats
	time.Sleep(500 * time.Millisecond)

	stats := server.Stats()
	t.Logf("Server stats: %+v", stats)
	t.Logf("Success: %d, Errors: %d, Active connections: %d", successCount, errorCount, atomic.LoadInt64(&activeConns))

	if successCount < numClients*9/10 {
		t.Errorf("too many failures: success %d/%d", successCount, numClients)
	}

	if atomic.LoadInt64(&activeConns) != 0 {
		t.Errorf("expected 0 active connections, got %d", atomic.LoadInt64(&activeConns))
	}
}

// TestKKWSLongRunning tests WebSocket server stability over time
func TestKKWSLongRunning(t *testing.T) {
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
	server := kkws.NewServer(addr, serverHandler, kknet.WithPoolSize(50))
	server.SetPath("/ws")

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Start()
	}()
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
					client := kkws.NewClient("ws://"+addr+"/ws", clientHandler)

					deadline := time.Now().Add(5 * time.Second)
					for {
						err := client.Connect()
						if err == nil {
							break
						}
						if time.Now().After(deadline) {
							return
						}
						time.Sleep(50 * time.Millisecond)
					}

					payload := []byte("test")
					if err := client.Send(payload); err != nil {
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

// TestKKWSHighThroughput tests high WebSocket message throughput
func TestKKWSHighThroughput(t *testing.T) {
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
	server := kkws.NewServer(addr, serverHandler, kknet.WithPoolSize(100))
	server.SetPath("/ws")

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Start()
	}()
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
			client := kkws.NewClient("ws://"+addr+"/ws", clientHandler)

			deadline := time.Now().Add(10 * time.Second)
			for {
				err := client.Connect()
				if err == nil {
					break
				}
				if time.Now().After(deadline) {
					return
				}
				time.Sleep(50 * time.Millisecond)
			}
			defer func() { _ = client.Close() }()

			for j := 0; j < messagesPerClient; j++ {
				payload := []byte{byte(j), byte(j >> 8)}
				if err := client.Send(payload); err != nil {
					return
				}
			}

			// Wait for all replies
			deadline = time.Now().Add(30 * time.Second)
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
