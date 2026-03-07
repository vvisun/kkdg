package kkgws

import (
	"fmt"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
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
		bb, err := kkpacket.DefaultStreamPacket().Pack(payload)
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
			msg, err := kkpacket.DefaultStreamPacket().Unpack(frame)
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
