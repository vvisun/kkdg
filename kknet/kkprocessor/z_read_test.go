package kkprocessor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

type collectingRawHandler struct {
	mu    sync.Mutex
	recvd []recvdPacket
}

type recvdPacket struct {
	connID kknet.CONN_ID
	data   []byte
}

func (h *collectingRawHandler) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	h.mu.Lock()
	defer h.mu.Unlock()
	cp := make([]byte, len(data.B))
	copy(cp, data.B)
	h.recvd = append(h.recvd, recvdPacket{connID: connID, data: cp})
}

func (h *collectingRawHandler) get() []recvdPacket {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]recvdPacket, len(h.recvd))
	copy(out, h.recvd)
	return out
}

func (h *collectingRawHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.recvd)
}

type mockConnForRead struct {
	id kknet.CONN_ID
}

func (m *mockConnForRead) ID() kknet.CONN_ID                     { return m.id }
func (m *mockConnForRead) Close() error                          { return nil }
func (m *mockConnForRead) RemoteAddr() string                    { return "mock:0" }
func (m *mockConnForRead) Context() context.Context              { return context.Background() }
func (m *mockConnForRead) SetContext(context.Context)            {}
func (m *mockConnForRead) SendBuffer(*kkbuffer.ByteBuffer) error { return nil }
func (m *mockConnForRead) SendMsg(any) error                     { return nil }
func (m *mockConnForRead) BindUser(kknet.USER_ID)                {}
func (m *mockConnForRead) UnbindUser()                           {}
func (m *mockConnForRead) GetUserId() kknet.USER_ID              { return kknet.NULL_USER_ID }

func TestReadProcessor_OnRecvBytes_SinglePacket(t *testing.T) {
	h := &collectingRawHandler{}
	opts := kknet.ReadOptions{
		RawHandler:       h,
		RecvQueueSize:    32,
		RecvQueueStrict:  false,
		RecvBufShrinkCap: 2048,
	}
	rp := NewReadProcessor(opts).(*ReadProcessor)
	conn := &mockConnForRead{id: 100}
	rp.Start(conn)
	defer rp.Stop()

	bb, err := kkpacket.DefaultStreamPacket().Pack([]byte("hello"))
	if err != nil {
		t.Fatalf("Pack: %v", err)
	}
	defer kkbuffer.Put(bb)

	err = rp.OnRecvBytes(bb.B)
	if err != nil {
		t.Fatalf("OnRecvBytes: %v", err)
	}

	time.Sleep(30 * time.Millisecond)
	recvd := h.get()
	if len(recvd) != 1 {
		t.Fatalf("expected 1 packet, got %d", len(recvd))
	}
	if recvd[0].connID != 100 {
		t.Errorf("connID = %d, want 100", recvd[0].connID)
	}
	msg, _ := kkpacket.DefaultStreamPacket().Unpack(recvd[0].data)
	if string(msg) != "hello" {
		t.Errorf("data = %q, want hello", msg)
	}
}

func TestReadProcessor_OnRecvBytes_MultiPacket(t *testing.T) {
	h := &collectingRawHandler{}
	opts := kknet.ReadOptions{
		RawHandler:       h,
		RecvQueueSize:    32,
		RecvQueueStrict:  false,
		RecvBufShrinkCap: 2048,
	}
	rp := NewReadProcessor(opts).(*ReadProcessor)
	conn := &mockConnForRead{id: 1}
	rp.Start(conn)
	defer rp.Stop()

	var combined []byte
	for _, msg := range []string{"a", "bb", "ccc"} {
		bb, err := kkpacket.DefaultStreamPacket().Pack([]byte(msg))
		if err != nil {
			t.Fatalf("Pack: %v", err)
		}
		combined = append(combined, bb.B...)
		kkbuffer.Put(bb)
	}

	err := rp.OnRecvBytes(combined)
	if err != nil {
		t.Fatalf("OnRecvBytes: %v", err)
	}

	time.Sleep(30 * time.Millisecond)
	recvd := h.get()
	if len(recvd) != 3 {
		t.Fatalf("expected 3 packets, got %d", len(recvd))
	}
	for i, want := range []string{"a", "bb", "ccc"} {
		msg, _ := kkpacket.DefaultStreamPacket().Unpack(recvd[i].data)
		if string(msg) != want {
			t.Errorf("packet %d: got %q, want %q", i, msg, want)
		}
	}
}

func TestReadProcessor_OnRecvBytes_PartialThenComplete(t *testing.T) {
	h := &collectingRawHandler{}
	opts := kknet.ReadOptions{
		RawHandler:       h,
		RecvQueueSize:    32,
		RecvQueueStrict:  false,
		RecvBufShrinkCap: 2048,
	}
	rp := NewReadProcessor(opts).(*ReadProcessor)
	conn := &mockConnForRead{id: 1}
	rp.Start(conn)
	defer rp.Stop()

	bb, err := kkpacket.DefaultStreamPacket().Pack([]byte("full"))
	if err != nil {
		t.Fatalf("Pack: %v", err)
	}
	defer kkbuffer.Put(bb)

	// 先发 length 字段前 2 字节（残包）
	err = rp.OnRecvBytes(bb.B[:2])
	if err != nil {
		t.Fatalf("OnRecvBytes partial: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	if h.count() != 0 {
		t.Errorf("partial should not produce packet, got %d", h.count())
	}

	// 补全
	err = rp.OnRecvBytes(bb.B[2:])
	if err != nil {
		t.Fatalf("OnRecvBytes complete: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	recvd := h.get()
	if len(recvd) != 1 {
		t.Fatalf("expected 1 packet after complete, got %d", len(recvd))
	}
	msg, _ := kkpacket.DefaultStreamPacket().Unpack(recvd[0].data)
	if string(msg) != "full" {
		t.Errorf("got %q, want full", msg)
	}
}

func TestReadProcessor_OnRecvBytes_Empty(t *testing.T) {
	h := &collectingRawHandler{}
	opts := kknet.ReadOptions{
		RawHandler:      h,
		RecvQueueSize:   32,
		RecvQueueStrict: false,
	}
	rp := NewReadProcessor(opts).(*ReadProcessor)
	conn := &mockConnForRead{id: 1}
	rp.Start(conn)
	defer rp.Stop()

	if err := rp.OnRecvBytes(nil); err != nil {
		t.Errorf("OnRecvBytes(nil): %v", err)
	}
	if err := rp.OnRecvBytes([]byte{}); err != nil {
		t.Errorf("OnRecvBytes([]): %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	if h.count() != 0 {
		t.Errorf("expected 0 packets, got %d", h.count())
	}
}

func TestReadProcessor_RecvQueueFullCallback(t *testing.T) {
	var fullCount int
	blockCh := make(chan struct{})
	opts := kknet.ReadOptions{
		RawHandler:       &blockingRawHandler{block: blockCh},
		RecvQueueSize:    2,
		RecvQueueStrict:  true,
		RecvBufShrinkCap: 2048,
		RecvQueueFullCallback: func(_ kknet.IConn) {
			fullCount++
		},
	}
	rp := NewReadProcessor(opts).(*ReadProcessor)
	conn := &mockConnForRead{id: 1}
	rp.Start(conn)
	defer func() {
		close(blockCh)
		rp.Stop()
	}()

	// 第 1 条：消费者 pop 后在 OnRaw 中阻塞，队列空
	bb1, _ := kkpacket.DefaultStreamPacket().Pack([]byte("1"))
	defer kkbuffer.Put(bb1)
	_ = rp.OnRecvBytes(bb1.B)
	time.Sleep(20 * time.Millisecond)

	// 第 2、3、4 条：队列 size=2，前 2 条填满，第 3 条 Push 失败触发 RecvQueueFullCallback
	bb2, _ := kkpacket.DefaultStreamPacket().Pack([]byte("2"))
	bb3, _ := kkpacket.DefaultStreamPacket().Pack([]byte("3"))
	bb4, _ := kkpacket.DefaultStreamPacket().Pack([]byte("4"))
	defer kkbuffer.Put(bb2)
	defer kkbuffer.Put(bb3)
	defer kkbuffer.Put(bb4)
	_ = rp.OnRecvBytes(append(append(bb2.B, bb3.B...), bb4.B...))
	time.Sleep(20 * time.Millisecond)

	if fullCount < 1 {
		t.Errorf("RecvQueueFullCallback should be called when queue full, got %d", fullCount)
	}
}

type blockingRawHandler struct {
	block chan struct{}
}

func (h *blockingRawHandler) OnRaw(_ kknet.CONN_ID, _ *kkbuffer.ByteBuffer) {
	<-h.block
}

func TestReadProcessor_EnqueuePacket(t *testing.T) {
	h := &collectingRawHandler{}
	opts := kknet.ReadOptions{
		RawHandler:       h,
		RecvQueueSize:    32,
		RecvQueueStrict:  false,
		RecvBufShrinkCap: 2048,
	}
	rp := NewReadProcessor(opts).(*ReadProcessor)
	conn := &mockConnForRead{id: 42}
	rp.Start(conn)
	defer rp.Stop()

	packet := []byte{0, 0, 0, 5, 'h', 'e', 'l', 'l', 'o'}
	rp.EnqueuePacket(packet)

	time.Sleep(30 * time.Millisecond)
	recvd := h.get()
	if len(recvd) != 1 {
		t.Fatalf("expected 1 packet, got %d", len(recvd))
	}
	if recvd[0].connID != 42 {
		t.Errorf("connID = %d, want 42", recvd[0].connID)
	}
	msg, _ := kkpacket.DefaultStreamPacket().Unpack(recvd[0].data)
	if string(msg) != "hello" {
		t.Errorf("data = %q, want hello", msg)
	}
}

func TestReadProcessor_EnqueuePacket_Empty(t *testing.T) {
	h := &collectingRawHandler{}
	opts := kknet.ReadOptions{
		RawHandler:    h,
		RecvQueueSize: 32,
	}
	rp := NewReadProcessor(opts).(*ReadProcessor)
	conn := &mockConnForRead{id: 1}
	rp.Start(conn)
	defer rp.Stop()

	rp.EnqueuePacket(nil)
	rp.EnqueuePacket([]byte{})
	time.Sleep(20 * time.Millisecond)
	if h.count() != 0 {
		t.Errorf("expected 0 packets, got %d", h.count())
	}
}

// syncCollectingHandler implements INoneCopyHandler for SyncReadProcessor tests.
type syncCollectingHandler struct {
	mu    sync.Mutex
	recvd []recvdPacket
}

func (h *syncCollectingHandler) OnNoneCopy(connID kknet.CONN_ID, data []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	h.recvd = append(h.recvd, recvdPacket{connID: connID, data: cp})
}

func (h *syncCollectingHandler) get() []recvdPacket {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]recvdPacket, len(h.recvd))
	copy(out, h.recvd)
	return out
}

func TestSyncReadProcessor_OnRecvBytes(t *testing.T) {
	h := &syncCollectingHandler{}
	opts := kknet.ReadOptions{
		NoneCopyHandler: h,
		RecvQueueSize:   32,
	}
	rp := NewSyncReadProcessor(opts).(*SyncReadProcessor)
	conn := &mockConnForRead{id: 7}
	rp.Start(conn)

	bb, err := kkpacket.DefaultStreamPacket().Pack([]byte("sync-hello"))
	if err != nil {
		t.Fatalf("Pack: %v", err)
	}
	defer kkbuffer.Put(bb)

	err = rp.OnRecvBytes(bb.B)
	if err != nil {
		t.Fatalf("OnRecvBytes: %v", err)
	}

	recvd := h.get()
	if len(recvd) != 1 {
		t.Fatalf("expected 1 packet, got %d", len(recvd))
	}
	if recvd[0].connID != 7 {
		t.Errorf("connID = %d, want 7", recvd[0].connID)
	}
	msg, _ := kkpacket.DefaultStreamPacket().Unpack(recvd[0].data)
	if string(msg) != "sync-hello" {
		t.Errorf("data = %q, want sync-hello", msg)
	}
}

func TestSyncReadProcessor_EnqueuePacket(t *testing.T) {
	h := &syncCollectingHandler{}
	opts := kknet.ReadOptions{
		NoneCopyHandler: h,
		RecvQueueSize:   32,
	}
	rp := NewSyncReadProcessor(opts).(*SyncReadProcessor)
	conn := &mockConnForRead{id: 99}
	rp.Start(conn)

	packet := []byte{0, 0, 0, 4, 't', 'e', 's', 't'}
	rp.EnqueuePacket(packet)

	recvd := h.get()
	if len(recvd) != 1 {
		t.Fatalf("expected 1 packet, got %d", len(recvd))
	}
	if recvd[0].connID != 99 {
		t.Errorf("connID = %d, want 99", recvd[0].connID)
	}
	msg, _ := kkpacket.DefaultStreamPacket().Unpack(recvd[0].data)
	if string(msg) != "test" {
		t.Errorf("data = %q, want test", msg)
	}
}

func TestReadProcessor_Stop_DrainsRemaining(t *testing.T) {
	h := &collectingRawHandler{}
	opts := kknet.ReadOptions{
		RawHandler:       h,
		RecvQueueSize:    32,
		RecvQueueStrict:  false,
		RecvBufShrinkCap: 2048,
	}
	rp := NewReadProcessor(opts).(*ReadProcessor)
	conn := &mockConnForRead{id: 1}
	rp.Start(conn)

	bb, _ := kkpacket.DefaultStreamPacket().Pack([]byte("drain"))
	defer kkbuffer.Put(bb)
	rp.EnqueuePacket(bb.B)

	rp.Stop()
	recvd := h.get()
	if len(recvd) != 1 {
		t.Errorf("Stop should drain remaining, got %d packets", len(recvd))
	}
}
