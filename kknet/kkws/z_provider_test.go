// 测试不同连接使用相同的 WP 或 RP（多连接共用同一处理器）
package kkws

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kkprocessor"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

func freePortProvider(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

type providerEchoRaw struct {
	ch chan []byte
}

func (h *providerEchoRaw) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	if data == nil {
		return
	}
	b := append([]byte(nil), data.Bytes()...)
	kkbuffer.Put(data)
	select {
	case h.ch <- b:
	default:
	}
}

func waitTCPReadyProvider(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("server not ready on %s after %v", addr, timeout)
}

// TestWSConn_SharedWP_MultiConn 多连接共用同一 WP，验证收发正常
func TestWSConn_SharedWP_MultiConn(t *testing.T) {
	addr := freePortProvider(t)
	recvCh := make(chan []byte, 32)
	h := &providerEchoRaw{ch: recvCh}

	var sharedWP atomic.Pointer[kkprocessor.SharedWriteProcessor]
	opts := kknet.ApplyOptions(
		kknet.WithRawHandler(h),
		kknet.WithSharedWpProvider(func(o kknet.WriteOptions) kknet.ISharedWriteProcessor {
			if wp := sharedWP.Load(); wp != nil {
				return wp
			}
			wp := kkprocessor.NewSharedWriteProcessor(o)
			wp.Start()
			sharedWP.Store(wp)
			return wp
		}),
	)

	s := NewServer(addr, nil, opts)
	s.Start()
	defer s.Stop()
	waitTCPReadyProvider(t, addr, 3*time.Second)
	time.Sleep(50 * time.Millisecond)

	clientOpts := kknet.ApplyOptions(kknet.WithRawHandler(&noopRawHandler{}))
	url := "ws://" + addr + "/ws"

	// 3 个客户端连接，共用同一 WP
	const nConns = 3
	var wg sync.WaitGroup
	for i := 0; i < nConns; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := NewClient(url, nil, clientOpts)
			if err := client.Connect(); err != nil {
				t.Errorf("conn %d Connect: %v", i, err)
				return
			}
			defer client.Close()

			payload := []byte("shared-" + string(rune('0'+i)))
			bb, err := kkpacket.DefaultStreamPacket().Pack(payload)
			if err != nil {
				t.Errorf("conn %d Pack: %v", i, err)
				return
			}
			if err := client.SendBuffer(bb); err != nil {
				t.Errorf("conn %d SendBuffer: %v", i, err)
				return
			}
		}()
	}
	wg.Wait()
	time.Sleep(100 * time.Millisecond)

	expected := map[string]bool{"shared-0": true, "shared-1": true, "shared-2": true}
	for i := 0; i < nConns; i++ {
		select {
		case got := <-recvCh:
			msg, err := kkpacket.DefaultStreamPacket().Unpack(got)
			if err != nil {
				t.Errorf("Unpack #%d: %v", i, err)
				continue
			}
			s := string(msg)
			if !expected[s] {
				t.Errorf("unexpected message %q", s)
			}
			delete(expected, s)
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for message #%d, received %d so far", i+1, i)
		}
	}
	if len(expected) != 0 {
		t.Errorf("missing messages: %v", expected)
	}

	// 停止共享 WP（所有连接已关闭，server Stop 会关连接）
	if wp := sharedWP.Load(); wp != nil {
		wp.Stop(nil)
	}
}
