// 压测：多连接/高吞吐/快速建连断连。
// 运行：go test -run TestStress -v -timeout=90s ./kknet/kkudp
// 跳过：go test -short 会跳过压测。
package kkudp

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

type stressRecvHandler struct {
	recvCount atomic.Int64
	ch        chan struct{}
	target    int64
	closeOnce sync.Once
}

func (h *stressRecvHandler) OnConnect(kknet.IConn)      {}
func (h *stressRecvHandler) OnClose(kknet.IConn, error) {}
func (h *stressRecvHandler) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	if data == nil {
		return
	}
	n := h.recvCount.Add(1)
	kkbuffer.Put(data)
	if h.target > 0 && h.ch != nil && n >= h.target {
		h.closeOnce.Do(func() { close(h.ch) })
	}
}

func (h *stressRecvHandler) Count() int64 { return h.recvCount.Load() }

// dummyLifecycle 使 server 进入 dispatch 分支（server 仅在 handler != nil 时调用 RawHandler）
type dummyLifecycle struct{}

func (dummyLifecycle) OnConnect(kknet.IConn)      {}
func (dummyLifecycle) OnClose(kknet.IConn, error) {}

type stressClientHandler struct{}

func (h *stressClientHandler) OnConnect(kknet.IConn)      {}
func (h *stressClientHandler) OnClose(kknet.IConn, error) {}
func (h *stressClientHandler) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	kkbuffer.Put(data)
}

// TestStress_ManyConns_ManyMessages 多客户端并发，每客户端发送多条消息。
func TestStress_ManyConns_ManyMessages(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	numConns := 666
	msgsPerConn := 8
	payload := make([]byte, 128)
	totalMsgs := int64(numConns * msgsPerConn)

	addr := freeUDPPort(t)
	recv := &stressRecvHandler{target: totalMsgs, ch: make(chan struct{})}
	serOpts := kknet.ApplyOptions(
		kknet.WithLogger(kklog.Nop()),
		kknet.WithRawHandler(recv),
	)
	srv := NewServer(addr, dummyLifecycle{}, serOpts)
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Stop()
	time.Sleep(100 * time.Millisecond)

	go func() {
		for {
			time.Sleep(1 * time.Second)
			stats := srv.Stats()
			kknet.PrintStress(&stats)
		}
	}()

	for i := range payload {
		payload[i] = 0x01
	}

	clientOpts := kknet.ApplyOptions(
		kknet.WithLogger(kklog.Nop()),
		kknet.WithRawHandler(&stressClientHandler{}),
	)
	start := time.Now()
	var wg sync.WaitGroup
	errCh := make(chan error, numConns)
	var clientsMu sync.Mutex
	clients := make([]*Client, 0, numConns)
	for i := 0; i < numConns; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := NewClient(addr, nil, clientOpts)
			if err := client.Connect(); err != nil {
				errCh <- err
				return
			}
			for j := 0; j < msgsPerConn; j++ {
				bb, err := kkpacket.DefaultStreamPacket().Pack(payload)
				if err != nil {
					errCh <- err
					return
				}
				if err := client.SendBuffer(bb); err != nil {
					errCh <- err
					return
				}
				if (j+1)%64 == 0 {
					time.Sleep(2 * time.Millisecond)
				}
			}
			clientsMu.Lock()
			clients = append(clients, client)
			clientsMu.Unlock()
		}()
	}
	wg.Wait()

	sendDone := time.Since(start)
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("stress send error: %v", err)
		}
	}

	select {
	case <-recv.ch:
		// 收齐
	case <-time.After(15 * time.Second):
		got := recv.Count()
		kklog.Debugf("stress: timeout %d/%d received", got, totalMsgs)
	}

	clientsMu.Lock()
	for _, c := range clients {
		_ = c.Close()
	}
	clientsMu.Unlock()

	got := recv.Count()
	elapsed := time.Since(start)
	kklog.Debugf("udp server received %d, total: %d, rate: %f", got, totalMsgs, float64(got)/float64(totalMsgs))
	kklog.Debugf("udp stress: send done in %v, all done in %v, recv/s ≈ %.0f",
		sendDone, elapsed, float64(got)/elapsed.Seconds())
	if got == 0 {
		t.Error("udp stress: server received 0 messages")
	}
}

// TestStress_ManyConns_ConnectDisconnect 快速建连/断连，压测连接生命周期。
func TestStress_ManyConns_ConnectDisconnect(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	rounds := 100
	connsPerRound := 20

	addr := freeUDPPort(t)
	srv := NewServer(addr, nil, kknet.DefaultOptions())
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Stop()
	time.Sleep(50 * time.Millisecond)

	start := time.Now()
	for r := 0; r < rounds; r++ {
		var wg sync.WaitGroup
		for i := 0; i < connsPerRound; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				client := NewClient(addr, nil, kknet.DefaultOptions())
				_ = client.Connect()
				time.Sleep(5 * time.Millisecond)
				_ = client.Close()
			}()
		}
		wg.Wait()
	}
	elapsed := time.Since(start)
	totalConns := rounds * connsPerRound
	kklog.Debugf("udp connect/disconnect: %d rounds × %d conns = %d total in %v, ≈ %.0f conn/s",
		rounds, connsPerRound, totalConns, elapsed, float64(totalConns)/elapsed.Seconds())
}
