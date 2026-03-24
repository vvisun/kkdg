package kkprocessor

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
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
func (m *mockConnForRead) SendBuffer(*kkbuffer.ByteBuffer) error { return nil }
func (m *mockConnForRead) SendMsg(any) error                     { return nil }

func TestReadProcessor_OnRecvBytes_SinglePacket(t *testing.T) {
	h := &collectingRawHandler{}
	opts := kknet.ApplyOptions(
		kknet.WithRawHandler(h),
		kknet.WithRecvQueueSize(32),
		kknet.WithRecvQueueStrict(false),
		kknet.WithRecvBufShrinkCap(2048),
	)
	rp := NewReadProcessor(opts.RpOptions).(*ReadProcessor)
	conn := &mockConnForRead{id: 100}
	rp.Start(conn)
	defer rp.Stop()

	bb, err := opts.StreamTool.Pack([]byte("hello"))
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
	msg, _ := opts.StreamTool.Unpack(recvd[0].data)
	if string(msg) != "hello" {
		t.Errorf("data = %q, want hello", msg)
	}
}

func TestReadProcessor_OnRecvBytes_MultiPacket(t *testing.T) {
	h := &collectingRawHandler{}
	opts := kknet.ApplyOptions(
		kknet.WithRawHandler(h),
		kknet.WithRecvQueueSize(32),
		kknet.WithRecvQueueStrict(false),
		kknet.WithRecvBufShrinkCap(2048),
	)
	rp := NewReadProcessor(opts.RpOptions).(*ReadProcessor)
	conn := &mockConnForRead{id: 1}
	rp.Start(conn)
	defer rp.Stop()

	var combined []byte
	for _, msg := range []string{"a", "bb", "ccc"} {
		bb, err := opts.StreamTool.Pack([]byte(msg))
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
		msg, _ := opts.StreamTool.Unpack(recvd[i].data)
		if string(msg) != want {
			t.Errorf("packet %d: got %q, want %q", i, msg, want)
		}
	}
}

func TestReadProcessor_OnRecvBytes_PartialThenComplete(t *testing.T) {
	h := &collectingRawHandler{}
	opts := kknet.ApplyOptions(
		kknet.WithRawHandler(h),
		kknet.WithRecvQueueSize(32),
		kknet.WithRecvQueueStrict(false),
		kknet.WithRecvBufShrinkCap(2048),
	)
	rp := NewReadProcessor(opts.RpOptions).(*ReadProcessor)
	conn := &mockConnForRead{id: 1}
	rp.Start(conn)
	defer rp.Stop()

	bb, err := opts.StreamTool.Pack([]byte("full"))
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
	msg, _ := opts.StreamTool.Unpack(recvd[0].data)
	if string(msg) != "full" {
		t.Errorf("got %q, want full", msg)
	}
}

func TestReadProcessor_OnRecvBytes_Empty(t *testing.T) {
	h := &collectingRawHandler{}
	opts := kknet.ApplyOptions(
		kknet.WithRawHandler(h),
		kknet.WithRecvQueueSize(32),
		kknet.WithRecvQueueStrict(false),
	)
	rp := NewReadProcessor(opts.RpOptions).(*ReadProcessor)
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
	opts := kknet.ApplyOptions(
		kknet.WithRawHandler(&blockingRawHandler{block: blockCh}),
		kknet.WithRecvQueueSize(2),
		kknet.WithRecvQueueStrict(true),
		kknet.WithRecvBufShrinkCap(2048),
		kknet.WithRecvQueueFullCallback(func(_ kknet.IConn) {
			fullCount++
			kklog.Infof("RecvQueueFullCallback: fullCount=%d", fullCount)
		}),
	)
	rp := NewReadProcessor(opts.RpOptions).(*ReadProcessor)
	conn := &mockConnForRead{id: 1}
	rp.Start(conn)
	defer func() {
		close(blockCh)
		rp.Stop()
	}()

	// 第 1 条：消费者 pop 后在 OnRaw 中阻塞，队列空
	bb1, _ := opts.StreamTool.Pack([]byte("1"))
	defer kkbuffer.Put(bb1)
	_ = rp.OnRecvBytes(bb1.B)
	time.Sleep(20 * time.Millisecond)

	// 第 2、3、4 条：队列 size=2，前 2 条填满，第 3 条 Push 失败触发 RecvQueueFullCallback
	bb2, _ := opts.StreamTool.Pack([]byte("2"))
	bb3, _ := opts.StreamTool.Pack([]byte("3"))
	bb4, _ := opts.StreamTool.Pack([]byte("4"))
	defer kkbuffer.Put(bb2)
	defer kkbuffer.Put(bb3)
	defer kkbuffer.Put(bb4)
	err := rp.OnRecvBytes(append(append(bb2.B, bb3.B...), bb4.B...))
	time.Sleep(20 * time.Millisecond)
	if err != kkerrors.ErrNetRecvQueueFull {
		t.Fatalf("OnRecvBytes = %v, want ErrNetRecvQueueFull", err)
	}

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

type signalBlockingRawHandler struct {
	entered chan struct{}
	release chan struct{}
	count   atomic.Int32
	once    sync.Once
}

func (h *signalBlockingRawHandler) OnRaw(_ kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	h.count.Add(1)
	h.once.Do(func() { close(h.entered) })
	<-h.release
	if data != nil {
		kkbuffer.Put(data)
	}
}

type orderedBlockingRawHandler struct {
	firstEntered  chan struct{}
	secondEntered chan struct{}
	release       chan struct{}
	count         atomic.Int32
}

func (h *orderedBlockingRawHandler) OnRaw(_ kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	n := h.count.Add(1)
	switch n {
	case 1:
		close(h.firstEntered)
		<-h.release
	case 2:
		close(h.secondEntered)
	}
	if data != nil {
		kkbuffer.Put(data)
	}
}

func TestReadProcessor_EnqueuePacket(t *testing.T) {
	h := &collectingRawHandler{}
	opts := kknet.ApplyOptions(
		kknet.WithRawHandler(h),
		kknet.WithRecvQueueSize(32),
		kknet.WithRecvQueueStrict(false),
		kknet.WithRecvBufShrinkCap(2048),
	)
	rp := NewReadProcessor(opts.RpOptions).(*ReadProcessor)
	conn := &mockConnForRead{id: 42}
	rp.Start(conn)
	defer rp.Stop()

	packet := []byte{0, 0, 0, 5, 'h', 'e', 'l', 'l', 'o'}
	if err := rp.EnqueuePacket(packet); err != nil {
		t.Fatalf("EnqueuePacket: %v", err)
	}

	time.Sleep(30 * time.Millisecond)
	recvd := h.get()
	if len(recvd) != 1 {
		t.Fatalf("expected 1 packet, got %d", len(recvd))
	}
	if recvd[0].connID != 42 {
		t.Errorf("connID = %d, want 42", recvd[0].connID)
	}
	msg, _ := opts.StreamTool.Unpack(recvd[0].data)
	if string(msg) != "hello" {
		t.Errorf("data = %q, want hello", msg)
	}
}

func TestReadProcessor_EnqueuePacket_Empty(t *testing.T) {
	h := &collectingRawHandler{}
	opts := kknet.ApplyOptions(
		kknet.WithRawHandler(h),
		kknet.WithRecvQueueSize(32),
		kknet.WithRecvQueueStrict(false),
	)
	rp := NewReadProcessor(opts.RpOptions).(*ReadProcessor)
	conn := &mockConnForRead{id: 1}
	rp.Start(conn)
	defer rp.Stop()

	if err := rp.EnqueuePacket(nil); err != nil {
		t.Fatalf("EnqueuePacket(nil): %v", err)
	}
	if err := rp.EnqueuePacket([]byte{}); err != nil {
		t.Fatalf("EnqueuePacket([]): %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	if h.count() != 0 {
		t.Errorf("expected 0 packets, got %d", h.count())
	}
}

func TestReadProcessor_EnqueuePacket_QueueFull_ReturnsError(t *testing.T) {
	var fullCount int
	blockCh := make(chan struct{})
	opts := kknet.ApplyOptions(
		kknet.WithRawHandler(&blockingRawHandler{block: blockCh}),
		kknet.WithRecvQueueSize(1),
		kknet.WithRecvQueueStrict(true),
		kknet.WithRecvQueueFullCallback(func(_ kknet.IConn) {
			fullCount++
		}),
	)
	rp := NewReadProcessor(opts.RpOptions).(*ReadProcessor)
	rp.Start(&mockConnForRead{id: 43})
	defer func() {
		close(blockCh)
		rp.Stop()
	}()

	if err := rp.EnqueuePacket([]byte{0, 0, 0, 1, 'a'}); err != nil {
		t.Fatalf("EnqueuePacket first: %v", err)
	}
	if err := rp.EnqueuePacket([]byte{0, 0, 0, 1, 'b'}); err != kkerrors.ErrNetRecvQueueFull {
		t.Fatalf("EnqueuePacket second = %v, want ErrNetRecvQueueFull", err)
	}
	if fullCount != 1 {
		t.Fatalf("RecvQueueFullCallback count = %d, want 1", fullCount)
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
	opts := kknet.ApplyOptions(
		kknet.WithNoneCopyHandler(h),
		kknet.WithRecvQueueSize(32),
	)
	rp := NewSyncReadProcessor(opts.RpOptions).(*SyncReadProcessor)
	conn := &mockConnForRead{id: 7}
	rp.Start(conn)

	bb, err := opts.StreamTool.Pack([]byte("sync-hello"))
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
	msg, _ := opts.StreamTool.Unpack(recvd[0].data)
	if string(msg) != "sync-hello" {
		t.Errorf("data = %q, want sync-hello", msg)
	}
}

func TestSyncReadProcessor_EnqueuePacket(t *testing.T) {
	h := &syncCollectingHandler{}
	opts := kknet.ApplyOptions(
		kknet.WithNoneCopyHandler(h),
		kknet.WithRecvQueueSize(32),
	)
	rp := NewSyncReadProcessor(opts.RpOptions).(*SyncReadProcessor)
	conn := &mockConnForRead{id: 99}
	rp.Start(conn)

	packet := []byte{0, 0, 0, 4, 't', 'e', 's', 't'}
	if err := rp.EnqueuePacket(packet); err != nil {
		t.Fatalf("EnqueuePacket: %v", err)
	}

	recvd := h.get()
	if len(recvd) != 1 {
		t.Fatalf("expected 1 packet, got %d", len(recvd))
	}
	if recvd[0].connID != 99 {
		t.Errorf("connID = %d, want 99", recvd[0].connID)
	}
	msg, _ := opts.StreamTool.Unpack(recvd[0].data)
	if string(msg) != "test" {
		t.Errorf("data = %q, want test", msg)
	}
}

func TestSyncReadProcessor_EnqueuePacket_QueueFull_ReturnsError(t *testing.T) {
	h := &syncBlockingHandler{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	opts := kknet.ApplyOptions(
		kknet.WithNoneCopyHandler(h),
		kknet.WithRecvQueueSize(1),
		kknet.WithRecvQueueStrict(true),
	)
	rp := NewSyncReadProcessor(opts.RpOptions).(*SyncReadProcessor)
	rp.Start(&mockConnForRead{id: 100})

	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		_ = rp.EnqueuePacket([]byte{0, 0, 0, 1, 'a'})
	}()

	select {
	case <-h.entered:
	case <-time.After(time.Second):
		t.Fatal("first packet did not enter none-copy handler")
	}

	if err := rp.EnqueuePacket([]byte{0, 0, 0, 1, 'b'}); err != kkerrors.ErrNetRecvQueueFull {
		t.Fatalf("EnqueuePacket second = %v, want ErrNetRecvQueueFull", err)
	}

	close(h.release)
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first enqueue did not complete")
	}
}

type syncBlockingHandler struct {
	entered chan struct{}
	release chan struct{}
	count   atomic.Int32
	once    sync.Once
}

func (h *syncBlockingHandler) OnNoneCopy(_ kknet.CONN_ID, _ []byte) {
	h.count.Add(1)
	h.once.Do(func() { close(h.entered) })
	<-h.release
}

func TestSyncReadProcessor_RecvQueueStrict_WithoutCallback_Drops(t *testing.T) {
	h := &syncBlockingHandler{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	opts := kknet.ApplyOptions(
		kknet.WithNoneCopyHandler(h),
		kknet.WithRecvQueueSize(1),
		kknet.WithRecvQueueStrict(true),
	)
	rp := NewSyncReadProcessor(opts.RpOptions).(*SyncReadProcessor)
	rp.Start(&mockConnForRead{id: 8})

	firstDone := make(chan struct{})
	go func() {
		rp.EnqueuePacket([]byte{0, 0, 0, 1, 'a'})
		close(firstDone)
	}()

	select {
	case <-h.entered:
	case <-time.After(time.Second):
		t.Fatal("first packet did not enter none-copy handler")
	}

	rp.EnqueuePacket([]byte{0, 0, 0, 1, 'b'})
	close(h.release)

	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first enqueue did not complete")
	}

	if got := h.count.Load(); got != 1 {
		t.Fatalf("handled %d packets, want 1", got)
	}
}

func TestSyncReadProcessor_RecvQueueStrict_ConcurrentAcquire_Drops(t *testing.T) {
	h := &syncBlockingHandler{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	opts := kknet.ApplyOptions(
		kknet.WithNoneCopyHandler(h),
		kknet.WithRecvQueueSize(1),
		kknet.WithRecvQueueStrict(true),
	)
	rp := NewSyncReadProcessor(opts.RpOptions).(*SyncReadProcessor)
	rp.Start(&mockConnForRead{id: 9})

	const workers = 16
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			rp.EnqueuePacket([]byte{0, 0, 0, 1, 'x'})
		}()
	}

	close(start)

	select {
	case <-h.entered:
	case <-time.After(time.Second):
		t.Fatal("no packet entered none-copy handler")
	}

	time.Sleep(50 * time.Millisecond)
	close(h.release)
	wg.Wait()

	if got := h.count.Load(); got != 1 {
		t.Fatalf("handled %d packets, want 1", got)
	}
}

func TestReadProcessor_Stop_DrainsRemaining(t *testing.T) {
	h := &collectingRawHandler{}
	opts := kknet.ApplyOptions(
		kknet.WithRawHandler(h),
		kknet.WithRecvQueueSize(32),
		kknet.WithRecvQueueStrict(false),
		kknet.WithRecvBufShrinkCap(2048),
	)
	rp := NewReadProcessor(opts.RpOptions).(*ReadProcessor)
	conn := &mockConnForRead{id: 1}
	rp.Start(conn)

	bb, _ := opts.StreamTool.Pack([]byte("drain"))
	defer kkbuffer.Put(bb)
	if err := rp.EnqueuePacket(bb.B); err != nil {
		t.Fatalf("EnqueuePacket: %v", err)
	}

	rp.Stop()
	recvd := h.get()
	if len(recvd) != 1 {
		t.Errorf("Stop should drain remaining, got %d packets", len(recvd))
	}
}

func TestReadProcessor_EnqueuePacket_AfterStop_Ignored(t *testing.T) {
	h := &collectingRawHandler{}
	opts := kknet.ApplyOptions(
		kknet.WithRawHandler(h),
		kknet.WithRecvQueueSize(32),
		kknet.WithRecvQueueStrict(false),
	)
	rp := NewReadProcessor(opts.RpOptions).(*ReadProcessor)
	rp.Start(&mockConnForRead{id: 1})

	rp.Stop()
	if err := rp.EnqueuePacket([]byte{0, 0, 0, 4, 'l', 'a', 't', 'e'}); err != nil {
		t.Fatalf("EnqueuePacket after Stop: %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	if got := h.count(); got != 0 {
		t.Fatalf("received %d packets after Stop, want 0", got)
	}
}

func TestReadProcessor_OnRecvBytes_AfterStop_Ignored(t *testing.T) {
	h := &collectingRawHandler{}
	opts := kknet.ApplyOptions(
		kknet.WithRawHandler(h),
		kknet.WithRecvQueueSize(32),
		kknet.WithRecvQueueStrict(false),
	)
	rp := NewReadProcessor(opts.RpOptions).(*ReadProcessor)
	rp.Start(&mockConnForRead{id: 3})

	bb, err := opts.StreamTool.Pack([]byte("late"))
	if err != nil {
		t.Fatalf("Pack: %v", err)
	}
	defer kkbuffer.Put(bb)

	rp.Stop()
	if err := rp.OnRecvBytes(bb.B); err != nil {
		t.Fatalf("OnRecvBytes after Stop: %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	if got := h.count(); got != 0 {
		t.Fatalf("received %d packets after Stop, want 0", got)
	}
}

func TestWorkerReadProcessor_EnqueuePacket_AfterStop_Ignored(t *testing.T) {
	h := &collectingRawHandler{}
	opts := kknet.ApplyOptions(
		kknet.WithRawHandler(h),
		kknet.WithRecvQueueSize(32),
		kknet.WithRecvQueueStrict(false),
	)
	opts.RpOptions.WorkerQueueMaxConcurrency = 1
	rp := NewWorkerReadProcessor(opts.RpOptions).(*WorkerReadProcessor)
	rp.Start(&mockConnForRead{id: 2})

	rp.Stop()
	if err := rp.EnqueuePacket([]byte{0, 0, 0, 4, 'l', 'a', 't', 'e'}); err != nil {
		t.Fatalf("EnqueuePacket after Stop: %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	if got := h.count(); got != 0 {
		t.Fatalf("worker received %d packets after Stop, want 0", got)
	}
}

func TestWorkerReadProcessor_RecvQueueStrict_WithoutCallback_Drops(t *testing.T) {
	h := &signalBlockingRawHandler{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	opts := kknet.ApplyOptions(
		kknet.WithRawHandler(h),
		kknet.WithRecvQueueSize(1),
		kknet.WithRecvQueueStrict(true),
	)
	opts.RpOptions.WorkerQueueMaxConcurrency = 1
	rp := NewWorkerReadProcessor(opts.RpOptions).(*WorkerReadProcessor)
	rp.Start(&mockConnForRead{id: 4})

	if err := rp.EnqueuePacket([]byte{0, 0, 0, 1, 'a'}); err != nil {
		t.Fatalf("EnqueuePacket first: %v", err)
	}
	select {
	case <-h.entered:
	case <-time.After(time.Second):
		t.Fatal("first packet did not enter handler")
	}

	if err := rp.EnqueuePacket([]byte{0, 0, 0, 1, 'b'}); err != kkerrors.ErrNetRecvQueueFull {
		t.Fatalf("EnqueuePacket second = %v, want ErrNetRecvQueueFull", err)
	}
	if err := rp.EnqueuePacket([]byte{0, 0, 0, 1, 'c'}); err != kkerrors.ErrNetRecvQueueFull {
		t.Fatalf("EnqueuePacket third = %v, want ErrNetRecvQueueFull", err)
	}

	close(h.release)
	rp.Stop()

	if got := h.count.Load(); got != 1 {
		t.Fatalf("handled %d packets, want 1", got)
	}
}

func TestWorkerReadProcessor_RecvQueueStrict_CountsRunningTasks(t *testing.T) {
	h := &signalBlockingRawHandler{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	opts := kknet.ApplyOptions(
		kknet.WithRawHandler(h),
		kknet.WithRecvQueueSize(1),
		kknet.WithRecvQueueStrict(true),
	)
	opts.RpOptions.WorkerQueueMaxConcurrency = 4
	rp := NewWorkerReadProcessor(opts.RpOptions).(*WorkerReadProcessor)
	rp.Start(&mockConnForRead{id: 6})

	const workers = 16
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_ = rp.EnqueuePacket([]byte{0, 0, 0, 1, 'x'})
		}()
	}

	close(start)

	select {
	case <-h.entered:
	case <-time.After(time.Second):
		t.Fatal("no packet entered handler")
	}

	time.Sleep(50 * time.Millisecond)
	if got := rp.Pending(); got != 1 {
		t.Fatalf("Pending() = %d, want 1 while first task is running", got)
	}

	close(h.release)
	wg.Wait()
	rp.Stop()

	if got := h.count.Load(); got != 1 {
		t.Fatalf("handled %d packets, want 1", got)
	}
	if got := rp.Pending(); got != 0 {
		t.Fatalf("Pending() after drain = %d, want 0", got)
	}
}

func TestWorkerReadProcessor_OnRecvBytes_QueueFull_ReturnsError(t *testing.T) {
	h := &signalBlockingRawHandler{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	opts := kknet.ApplyOptions(
		kknet.WithRawHandler(h),
		kknet.WithRecvQueueSize(1),
		kknet.WithRecvQueueStrict(true),
	)
	opts.RpOptions.WorkerQueueMaxConcurrency = 1
	rp := NewWorkerReadProcessor(opts.RpOptions).(*WorkerReadProcessor)
	rp.Start(&mockConnForRead{id: 10})

	bb1, err := opts.StreamTool.Pack([]byte("a"))
	if err != nil {
		t.Fatalf("Pack first: %v", err)
	}
	defer kkbuffer.Put(bb1)
	if err := rp.OnRecvBytes(bb1.B); err != nil {
		t.Fatalf("OnRecvBytes first = %v", err)
	}

	select {
	case <-h.entered:
	case <-time.After(time.Second):
		t.Fatal("first packet did not enter handler")
	}

	bb2, err := opts.StreamTool.Pack([]byte("b"))
	if err != nil {
		t.Fatalf("Pack second: %v", err)
	}
	defer kkbuffer.Put(bb2)
	bb3, err := opts.StreamTool.Pack([]byte("c"))
	if err != nil {
		t.Fatalf("Pack third: %v", err)
	}
	defer kkbuffer.Put(bb3)

	err = rp.OnRecvBytes(append(append([]byte{}, bb2.B...), bb3.B...))
	if err != kkerrors.ErrNetRecvQueueFull {
		t.Fatalf("OnRecvBytes combined = %v, want ErrNetRecvQueueFull", err)
	}

	close(h.release)
	rp.Stop()
}

func TestWorkerReadProcessor_Stop_DrainsAcceptedTasks(t *testing.T) {
	h := &orderedBlockingRawHandler{
		firstEntered:  make(chan struct{}),
		secondEntered: make(chan struct{}),
		release:       make(chan struct{}),
	}
	opts := kknet.ApplyOptions(
		kknet.WithRawHandler(h),
		kknet.WithRecvQueueSize(8),
		kknet.WithRecvQueueStrict(false),
	)
	opts.RpOptions.WorkerQueueMaxConcurrency = 1
	rp := NewWorkerReadProcessor(opts.RpOptions).(*WorkerReadProcessor)
	rp.Start(&mockConnForRead{id: 5})

	if err := rp.EnqueuePacket([]byte{0, 0, 0, 1, 'a'}); err != nil {
		t.Fatalf("EnqueuePacket first: %v", err)
	}
	select {
	case <-h.firstEntered:
	case <-time.After(time.Second):
		t.Fatal("first packet did not enter handler")
	}

	if err := rp.EnqueuePacket([]byte{0, 0, 0, 1, 'b'}); err != nil {
		t.Fatalf("EnqueuePacket second: %v", err)
	}

	stopDone := make(chan struct{})
	go func() {
		rp.Stop()
		close(stopDone)
	}()

	select {
	case <-stopDone:
		t.Fatal("Stop returned before accepted tasks drained")
	case <-time.After(50 * time.Millisecond):
	}

	close(h.release)

	select {
	case <-h.secondEntered:
	case <-time.After(time.Second):
		t.Fatal("second accepted task was not drained during Stop")
	}

	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("Stop did not return after accepted tasks drained")
	}

	if got := h.count.Load(); got != 2 {
		t.Fatalf("handled %d packets, want 2", got)
	}
}
