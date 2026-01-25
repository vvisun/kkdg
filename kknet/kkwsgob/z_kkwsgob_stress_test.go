package kkwsgob

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet"
)

func TestKKWSGobHighConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	addr := freeTCPAddr(t)

	var activeConns int64
	serverHandler := &gobTestHandler{
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
	server := NewServer(addr, serverHandler, kknet.WithPoolSize(100))
	server.SetPath("/ws")

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Start()
	}()
	defer func() { _ = server.Stop() }()

	const numClients = 200
	var wg sync.WaitGroup
	var successCount int64
	var errorCount int64

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			msgCh := make(chan []byte, 1)
			clientHandler := &gobTestHandler{
				onMessage: func(c kknet.IConn, data []byte) {
					select {
					case msgCh <- data:
					default:
					}
				},
			}
			client := NewClient("ws://"+addr+"/ws", clientHandler)

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
	time.Sleep(500 * time.Millisecond)

	stats := server.Stats()
	t.Logf("Server stats: %+v", stats)
	t.Logf("Success: %d, Errors: %d, Active connections: %d", successCount, errorCount, atomic.LoadInt64(&activeConns))

	if successCount < int64(numClients)*9/10 {
		t.Errorf("too many failures: success %d/%d", successCount, numClients)
	}

	if atomic.LoadInt64(&activeConns) != 0 {
		t.Errorf("expected 0 active connections, got %d", activeConns)
	}
}
