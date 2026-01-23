package kktcp

import (
	"net"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

func TestClientFlushTimeoutCallback(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer func() { _ = serverConn.Close() }()

	timeout := 50 * time.Millisecond
	callbackCh := make(chan struct{}, 1)

	opts := kknet.ApplyOptions(
		kknet.WithBufferSizes(0, 1024*1024),
		kknet.WithTcpClientNeedFlushOver(true),
		kknet.WithTimeoutTcpFlushOver(timeout),
		kknet.WithTcpClientFlushTimeoutCallback(func(conn kknet.IConn, flushTimeout time.Duration) {
			if flushTimeout != timeout {
				t.Errorf("flush timeout mismatch: got %v want %v", flushTimeout, timeout)
			}
			callbackCh <- struct{}{}
		}),
	)

	stats := &kknet.Stats{}
	c := newClientConn(clientConn, opts, stats)

	done := make(chan struct{})
	go func() {
		_ = c.writeLoop()
		close(done)
	}()

	payload := make([]byte, 4*128*1024)
	kkpacket.GetByteOrder().PutUint32(payload[:4], uint32(len(payload)))
	bb := &kkbuffer.ByteBuffer{B: payload}
	for i := 0; i < 6; i++ {
		if err := c.SendBuffer(bb); err != nil {
			t.Fatalf("send failed: %v", err)
		}
	}

	if err := c.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	select {
	case <-callbackCh:
	case <-time.After(2 * timeout):
		t.Fatalf("flush timeout callback not fired")
	}

	select {
	case <-done:
	case <-time.After(2 * timeout):
		t.Fatalf("write loop did not exit after timeout")
	}

	if stats.Snapshot().Errors == 0 {
		t.Fatalf("expected error count to increase on flush timeout")
	}
}
