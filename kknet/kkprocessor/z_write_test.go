package kkprocessor

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

// mockConn implements kknet.IConn for testing.
type mockConn struct {
	id kknet.CONN_ID
}

func (m *mockConn) ID() kknet.CONN_ID                       { return m.id }
func (m *mockConn) Close() error                            { return nil }
func (m *mockConn) RemoteAddr() string                      { return "mock:0" }
func (m *mockConn) SendBuffer(_ *kkbuffer.ByteBuffer) error { return nil }
func (m *mockConn) SendMsg(_ any) error                     { return nil }

func TestWriteProcessor_DropMode(t *testing.T) {
	opts := kknet.WriteOptions{
		SendQueueSize:          2,
		SendQueueStrict:        true,
		SendQueueFullAction:    kknet.EWpQueueFullActionDrop,
		SendQueueNeedFlushOver: false,
		BatchWriteLimitBytes:   1024,
	}
	wp := NewWriteProcessor(opts).(*WriteProcessor)

	blockFirst := make(chan struct{})
	var firstDone atomic.Bool
	writeFn := func(batch []*kkbuffer.ByteBuffer, n int) error {
		for i := 0; i < n; i++ {
			if batch[i] != nil {
				kkbuffer.Put(batch[i])
				batch[i] = nil
			}
		}
		if !firstDone.Swap(true) {
			<-blockFirst
		}
		return nil
	}
	wp.Start(&mockConn{id: 1}, writeFn, nil, nil)

	for i := 0; i < 2; i++ {
		bb := kkbuffer.GetWithCapacity(8)
		bb.B = bb.B[:8]
		if err := wp.SendBuffer(bb); err != nil {
			t.Fatalf("SendBuffer #%d: %v", i+1, err)
		}
	}
	time.Sleep(5 * time.Millisecond)

	for i := 0; i < 2; i++ {
		bb := kkbuffer.GetWithCapacity(8)
		bb.B = bb.B[:8]
		if err := wp.SendBuffer(bb); err != nil {
			t.Fatalf("SendBuffer #%d (refill): %v", i+3, err)
		}
	}

	bb := kkbuffer.GetWithCapacity(8)
	bb.B = bb.B[:8]
	err := wp.SendBuffer(bb)
	if err != nil {
		t.Errorf("Drop mode should return nil when queue full, got %v", err)
	}

	blockFirst <- struct{}{}
	wp.Stop(nil)
}

func TestWriteProcessor_RetryMode(t *testing.T) {
	opts := kknet.WriteOptions{
		SendQueueSize:          2,
		SendQueueStrict:        true,
		SendQueueFullAction:    kknet.EWpQueueFullActionRetry,
		SendQueueRetryInterval: 2 * time.Millisecond,
		SendQueueRetryMaxCount: 3,
		SendQueueNeedFlushOver: false,
		BatchWriteLimitBytes:   1024,
	}
	wp := NewWriteProcessor(opts).(*WriteProcessor)

	blockFirst := make(chan struct{})
	var firstDone atomic.Bool
	writeFn := func(batch []*kkbuffer.ByteBuffer, n int) error {
		for i := 0; i < n; i++ {
			if batch[i] != nil {
				kkbuffer.Put(batch[i])
				batch[i] = nil
			}
		}
		if !firstDone.Swap(true) {
			<-blockFirst
		}
		return nil
	}
	wp.Start(&mockConn{id: 1}, writeFn, nil, nil)

	for i := 0; i < 2; i++ {
		bb := kkbuffer.GetWithCapacity(8)
		bb.B = bb.B[:8]
		if err := wp.SendBuffer(bb); err != nil {
			t.Fatalf("SendBuffer #%d: %v", i+1, err)
		}
	}
	time.Sleep(5 * time.Millisecond)

	for i := 0; i < 2; i++ {
		bb := kkbuffer.GetWithCapacity(8)
		bb.B = bb.B[:8]
		if err := wp.SendBuffer(bb); err != nil {
			t.Fatalf("SendBuffer #%d (refill): %v", i+3, err)
		}
	}

	bb := kkbuffer.GetWithCapacity(8)
	bb.B = bb.B[:8]
	errCh := make(chan error, 1)
	go func() {
		errCh <- wp.SendBuffer(bb)
	}()

	time.Sleep(1 * time.Millisecond)
	select {
	case err := <-errCh:
		t.Fatalf("SendBuffer should not return before retries exhausted: %v", err)
	default:
	}

	select {
	case err := <-errCh:
		if err != kkerrors.ErrNetSendQueueFull {
			t.Errorf("SendBuffer after max retries: got %v, want ErrSendQueueFull", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("SendBuffer should return ErrSendQueueFull after max retries")
	}

	blockFirst <- struct{}{}
	wp.Stop(nil)
}

func TestWriteProcessor_WriteFnRetry_Retryable(t *testing.T) {
	opts := kknet.WriteOptions{
		SendQueueSize:          16,
		SendQueueStrict:        false,
		WriteFnRetryMaxCount:   5,
		WriteFnRetryInterval:   2 * time.Millisecond,
		SendQueueNeedFlushOver: false,
		BatchWriteLimitBytes:   1024,
	}
	wp := NewWriteProcessor(opts).(*WriteProcessor)

	var attempts atomic.Int32
	writeFn := func(batch []*kkbuffer.ByteBuffer, n int) error {
		a := attempts.Add(1)
		if a < 3 {
			return errors.New("temporary failure")
		}
		for i := 0; i < n; i++ {
			if batch[i] != nil {
				kkbuffer.Put(batch[i])
				batch[i] = nil
			}
		}
		return nil
	}
	var onErr error
	wp.Start(&mockConn{id: 1}, writeFn, func(e error) { onErr = e }, nil)

	bb := kkbuffer.GetWithCapacity(8)
	bb.B = bb.B[:8]
	if err := wp.SendBuffer(bb); err != nil {
		t.Fatalf("SendBuffer: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	if attempts.Load() < 3 {
		t.Errorf("writeFn should be retried at least 3 times, got %d", attempts.Load())
	}
	if onErr != nil {
		t.Errorf("onWriteError should not be called on retryable success, got %v", onErr)
	}

	wp.Stop(nil)
}

func TestWriteProcessor_WriteFnRetry_NonRetryable(t *testing.T) {
	opts := kknet.WriteOptions{
		SendQueueSize:          16,
		SendQueueStrict:        false,
		WriteFnRetryMaxCount:   5,
		WriteFnRetryInterval:   2 * time.Millisecond,
		SendQueueNeedFlushOver: false,
		BatchWriteLimitBytes:   1024,
	}
	wp := NewWriteProcessor(opts).(*WriteProcessor)

	var attempts atomic.Int32
	writeFn := func(batch []*kkbuffer.ByteBuffer, n int) error {
		attempts.Add(1)
		return kkerrors.ErrNetConnectionClosed
	}
	var onErr error
	var onErrOnce sync.Once
	wp.Start(&mockConn{id: 1}, writeFn, func(e error) {
		onErrOnce.Do(func() { onErr = e })
	}, nil)

	bb := kkbuffer.GetWithCapacity(8)
	bb.B = bb.B[:8]
	if err := wp.SendBuffer(bb); err != nil {
		t.Fatalf("SendBuffer: %v", err)
	}

	<-wp.Done()
	if attempts.Load() != 1 {
		t.Errorf("writeFn should not be retried for ErrConnectionClosed, got %d attempts", attempts.Load())
	}
	if onErr != kkerrors.ErrNetConnectionClosed {
		t.Errorf("onWriteError: got %v, want ErrConnectionClosed", onErr)
	}
}

func TestWriteProcessor_WriteFnRetry_CustomIsRetryable(t *testing.T) {
	opts := kknet.WriteOptions{
		SendQueueSize:          16,
		SendQueueStrict:        false,
		WriteFnRetryMaxCount:   3,
		WriteFnRetryInterval:   1 * time.Millisecond,
		SendQueueNeedFlushOver: false,
		BatchWriteLimitBytes:   1024,
		WriteFnIsRetryable: func(err error) bool {
			return err != nil && err.Error() == "retry-me"
		},
	}
	wp := NewWriteProcessor(opts).(*WriteProcessor)

	var attempts atomic.Int32
	writeFn := func(batch []*kkbuffer.ByteBuffer, n int) error {
		a := attempts.Add(1)
		if a < 2 {
			return errors.New("retry-me")
		}
		for i := 0; i < n; i++ {
			if batch[i] != nil {
				kkbuffer.Put(batch[i])
				batch[i] = nil
			}
		}
		return nil
	}
	var onErr error
	wp.Start(&mockConn{id: 1}, writeFn, func(e error) { onErr = e }, nil)

	bb := kkbuffer.GetWithCapacity(8)
	bb.B = bb.B[:8]
	if err := wp.SendBuffer(bb); err != nil {
		t.Fatalf("SendBuffer: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	if attempts.Load() < 2 {
		t.Errorf("writeFn should be retried, got %d attempts", attempts.Load())
	}
	if onErr != nil {
		t.Errorf("onWriteError should not be called on retryable success, got %v", onErr)
	}

	wp.Stop(nil)
}

func TestWriteProcessor_Pending(t *testing.T) {
	opts := kknet.WriteOptions{
		SendQueueSize:          8,
		SendQueueStrict:        false,
		SendQueueNeedFlushOver: false,
		BatchWriteLimitBytes:   1024,
	}
	wp := NewWriteProcessor(opts).(*WriteProcessor)

	writeFn := func(batch []*kkbuffer.ByteBuffer, n int) error {
		for i := 0; i < n; i++ {
			if batch[i] != nil {
				kkbuffer.Put(batch[i])
				batch[i] = nil
			}
		}
		return nil
	}
	wp.Start(&mockConn{id: 1}, writeFn, nil, nil)

	if p := wp.Pending(); p != 0 {
		t.Errorf("Pending() before send = %d, want 0", p)
	}

	for i := 0; i < 3; i++ {
		bb := kkbuffer.GetWithCapacity(8)
		bb.B = bb.B[:8]
		if err := wp.SendBuffer(bb); err != nil {
			t.Fatalf("SendBuffer: %v", err)
		}
	}
	wp.Stop(nil)
	// Stop 后队列应已排空
	if p := wp.Pending(); p != 0 {
		t.Errorf("Pending() after Stop = %d, want 0", p)
	}
}

func TestWriteProcessor_SendMsg_UnregisteredType(t *testing.T) {
	router := kkpacket.NewMsgRouter()
	codec := kkcodec.GetCodec(kkcodec.CodecTypeJson)
	msgPacket := kkpacket.NewMessagePacket(kkpacket.NewPacketHead(&kkpacket.PartUint32{}), codec, router)
	// router 未注册 "string" 类型，GetMsgID 返回 0

	opts := kknet.WriteOptions{
		SendQueueSize:          8,
		SendQueueNeedFlushOver: false,
		BatchWriteLimitBytes:   1024,
		MsgPacket:              msgPacket,
	}
	wp := NewWriteProcessor(opts).(*WriteProcessor)
	writeFn := func(batch []*kkbuffer.ByteBuffer, n int) error {
		for i := 0; i < n; i++ {
			if batch[i] != nil {
				kkbuffer.Put(batch[i])
				batch[i] = nil
			}
		}
		return nil
	}
	wp.Start(&mockConn{id: 1}, writeFn, nil, nil)
	defer wp.Stop(nil)

	err := wp.SendMsg("unregistered-string")
	if err == nil {
		t.Fatal("SendMsg with unregistered type should return error")
	}
	if !errors.Is(err, kkerrors.ErrPktMsgTypeNotRegistered) {
		t.Errorf("SendMsg: got %v, want ErrMsgTypeNotRegistered", err)
	}
}

func TestWriteProcessor_DefaultAction_Unknown(t *testing.T) {
	opts := kknet.WriteOptions{
		SendQueueSize:          2,
		SendQueueStrict:        true,
		SendQueueFullAction:    kknet.EWpQueueFullAction(99), // 未知 action
		SendQueueNeedFlushOver: false,
		BatchWriteLimitBytes:   1024,
	}
	wp := NewWriteProcessor(opts).(*WriteProcessor)

	writeFn := func(batch []*kkbuffer.ByteBuffer, n int) error {
		for i := 0; i < n; i++ {
			if batch[i] != nil {
				kkbuffer.Put(batch[i])
				batch[i] = nil
			}
		}
		return nil
	}
	wp.Start(&mockConn{id: 1}, writeFn, nil, nil)

	for i := 0; i < 4; i++ {
		bb := kkbuffer.GetWithCapacity(8)
		bb.B = bb.B[:8]
		if err := wp.SendBuffer(bb); err != nil {
			t.Fatalf("SendBuffer #%d: %v", i+1, err)
		}
	}
	time.Sleep(5 * time.Millisecond)

	bb := kkbuffer.GetWithCapacity(8)
	bb.B = bb.B[:8]
	err := wp.SendBuffer(bb)
	if err != nil {
		t.Errorf("default/unknown action: queue full should return nil (drop), got %v", err)
	}

	wp.Stop(nil)
}

func TestWriteProcessor_FlushTimeout(t *testing.T) {
	opts := kknet.WriteOptions{
		SendQueueSize:          4,
		SendQueueStrict:        false,
		SendQueueNeedFlushOver: true,
		// 小于 500ms 会被 CheckWriteOptions 提升到 500ms，这里直接使用 500ms 方便断言
		SendQueueTimeoutFlushOver: 500 * time.Millisecond,
		BatchWriteLimitBytes:      1024,
	}
	flushTimeoutCh := make(chan time.Duration, 1)
	opts.SendQueueFlushTimeoutCallback = func(conn kknet.IConn, timeout time.Duration) {
		select {
		case flushTimeoutCh <- timeout:
		default:
		}
	}
	wp := NewWriteProcessor(opts).(*WriteProcessor)

	writeFn := func(batch []*kkbuffer.ByteBuffer, n int) error {
		// 写入时间需要明显大于超时时间，确保触发 flush 超时逻辑
		time.Sleep(600 * time.Millisecond)
		for i := 0; i < n; i++ {
			if batch[i] != nil {
				kkbuffer.Put(batch[i])
				batch[i] = nil
			}
		}
		return nil
	}
	conn := &mockConn{id: 1}
	wp.Start(conn, writeFn, nil, nil)

	for i := 0; i < 2; i++ {
		bb := kkbuffer.GetWithCapacity(8)
		bb.B = bb.B[:8]
		if err := wp.SendBuffer(bb); err != nil {
			t.Fatalf("SendBuffer: %v", err)
		}
	}

	done := make(chan struct{})
	go func() {
		wp.Stop(nil)
		close(done)
	}()

	select {
	case to := <-flushTimeoutCh:
		if to < 500*time.Millisecond {
			t.Errorf("flush timeout callback: got %v, want >= 500ms", to)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("flush timeout callback should be invoked")
	}

	<-done
}

func TestWorkerWriteProcessor_FlushTimeout_ReturnsWithoutWaitingDone(t *testing.T) {
	opts := kknet.WriteOptions{
		SendQueueSize:             4,
		SendQueueStrict:           false,
		SendQueueNeedFlushOver:    true,
		SendQueueTimeoutFlushOver: 500 * time.Millisecond,
		BatchWriteLimitBytes:      1024,
	}
	flushTimeoutCh := make(chan time.Duration, 1)
	opts.SendQueueFlushTimeoutCallback = func(conn kknet.IConn, timeout time.Duration) {
		select {
		case flushTimeoutCh <- timeout:
		default:
		}
	}
	wp := NewWorkerWriteProcessor(opts).(*WorkerWriteProcessor)

	blockWrite := make(chan struct{})
	writeFn := func(batch []*kkbuffer.ByteBuffer, n int) error {
		<-blockWrite
		for i := 0; i < n; i++ {
			if batch[i] != nil {
				kkbuffer.Put(batch[i])
				batch[i] = nil
			}
		}
		return nil
	}
	conn := &mockConn{id: 2}
	wp.Start(conn, writeFn, nil, nil)

	for i := 0; i < 2; i++ {
		bb := kkbuffer.GetWithCapacity(8)
		bb.B = bb.B[:8]
		if err := wp.SendBuffer(bb); err != nil {
			t.Fatalf("SendBuffer: %v", err)
		}
	}

	done := make(chan struct{})
	go func() {
		wp.Stop(nil)
		close(done)
	}()

	select {
	case to := <-flushTimeoutCh:
		if to < 500*time.Millisecond {
			t.Errorf("flush timeout callback: got %v, want >= 500ms", to)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("flush timeout callback should be invoked")
	}

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("WorkerWriteProcessor.Stop should return shortly after flush timeout")
	}

	close(blockWrite)

	select {
	case <-wp.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("worker writer should finish after blocked write is released")
	}
}
