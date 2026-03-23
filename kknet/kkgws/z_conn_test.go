package kkgws

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/queues/bbqueue"
)

// orderRecvHandler 按序接收消息并写入 channel。
type orderRecvHandler struct {
	ch chan []byte
}

func (h *orderRecvHandler) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	if data == nil {
		return
	}
	b := append([]byte(nil), data.Bytes()...)
	select {
	case h.ch <- b:
	default:
	}
	kkbuffer.Put(data)
}

// TestConn_SendBuffer_Order 客户端顺序发送多条消息，服务端按序接收（对齐 kkws TestWSConn_AsyncSend_Order）。
func TestConn_SendBuffer_Order(t *testing.T) {
	addr := freePort(t)
	recvCh := make(chan []byte, 64)
	opts := kknet.ApplyOptions(kknet.WithRawHandler(&orderRecvHandler{ch: recvCh}))
	s := NewServer(addr, nil, opts)
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer s.Stop()

	client := NewClient("ws://"+addr+"/ws", nil, opts)
	if err := client.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Close()
	time.Sleep(50 * time.Millisecond)

	const n = 50
	for i := 0; i < n; i++ {
		payload := []byte(fmt.Sprintf("m%d", i))
		bb, err := opts.StreamTool.Pack(payload)
		if err != nil {
			t.Fatalf("Pack: %v", err)
		}
		if err := client.SendBuffer(bb); err != nil {
			t.Fatalf("SendBuffer: %v", err)
		}
	}

	for i := 0; i < n; i++ {
		select {
		case frame := <-recvCh:
			msg, err := opts.StreamTool.Unpack(frame)
			if err != nil {
				t.Fatalf("Unpack: %v", err)
			}
			want := fmt.Sprintf("m%d", i)
			if string(msg) != want {
				t.Fatalf("got %q, want %q", string(msg), want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for message %d", i)
		}
	}
}

func TestConn_HandleWriteError_ClosesAndDropsQueue(t *testing.T) {
	opts := kknet.ApplyOptions()
	closedCh := make(chan error, 1)
	handler := &testHandler{
		onClose: func(c kknet.IConn, err error) {
			closedCh <- err
		},
	}
	c := &gwsConn{
		opts:      &opts,
		handler:   handler,
		sendQueue: bbqueue.NewBBQueue(8, false),
	}
	c.closeCond = sync.NewCond(&c.closeMu)

	for i := 0; i < 3; i++ {
		bb := kkbuffer.GetWithCapacity(8)
		bb.B = append(bb.B, byte(i))
		if !c.sendQueue.Push(bb) {
			t.Fatalf("push %d failed", i)
		}
	}

	writeErr := errors.New("write failed")
	c.handleWriteError(writeErr)

	if !c.closing.Load() {
		t.Fatal("closing should be true after write error")
	}
	if got := c.sendQueue.Len(); got != 0 {
		t.Fatalf("sendQueue.Len() = %d, want 0", got)
	}

	select {
	case err := <-closedCh:
		if !errors.Is(err, writeErr) {
			t.Fatalf("OnClose err = %v, want %v", err, writeErr)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for OnClose")
	}
}
