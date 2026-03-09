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
	wp.Start(&mockConn{id: 1}, writeFn, nil)

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

func TestWriteProcessor_BlockMode(t *testing.T) {
	opts := kknet.WriteOptions{
		SendQueueSize:          2,
		SendQueueStrict:        true,
		SendQueueFullAction:    kknet.EWpQueueFullActionBlock,
		SendQueueNeedFlushOver: false,
		BatchWriteLimitBytes:   1024,
		SendQueueRetryInterval: 2 * time.Millisecond,
		SendQueueRetryMaxCount: 10,
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
	wp.Start(&mockConn{id: 1}, writeFn, nil)

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

	blocked := make(chan struct{})
	var blockErr error
	go func() {
		bb := kkbuffer.GetWithCapacity(8)
		bb.B = bb.B[:8]
		blockErr = wp.SendBuffer(bb)
		close(blocked)
	}()

	select {
	case <-blocked:
		t.Fatal("SendBuffer should block when queue full")
	case <-time.After(50 * time.Millisecond):
	}

	blockFirst <- struct{}{}
	wp.Stop(kkerrors.ErrConnectionClosed)

	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("SendBuffer should unblock after Stop")
	}
	if blockErr != kkerrors.ErrConnectionClosed {
		t.Errorf("blocked SendBuffer after Stop: got %v, want ErrConnectionClosed", blockErr)
	}
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
	wp.Start(&mockConn{id: 1}, writeFn, nil)

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
		if err != kkerrors.ErrSendQueueFull {
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
	wp.Start(&mockConn{id: 1}, writeFn, func(e error) { onErr = e })

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
		return kkerrors.ErrConnectionClosed
	}
	var onErr error
	var onErrOnce sync.Once
	wp.Start(&mockConn{id: 1}, writeFn, func(e error) {
		onErrOnce.Do(func() { onErr = e })
	})

	bb := kkbuffer.GetWithCapacity(8)
	bb.B = bb.B[:8]
	if err := wp.SendBuffer(bb); err != nil {
		t.Fatalf("SendBuffer: %v", err)
	}

	<-wp.Done()
	if attempts.Load() != 1 {
		t.Errorf("writeFn should not be retried for ErrConnectionClosed, got %d attempts", attempts.Load())
	}
	if onErr != kkerrors.ErrConnectionClosed {
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
	wp.Start(&mockConn{id: 1}, writeFn, func(e error) { onErr = e })

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
	wp.Start(&mockConn{id: 1}, writeFn, nil)

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
	wp.Start(&mockConn{id: 1}, writeFn, nil)
	defer wp.Stop(nil)

	err := wp.SendMsg("unregistered-string")
	if err == nil {
		t.Fatal("SendMsg with unregistered type should return error")
	}
	if !errors.Is(err, kkerrors.ErrMsgTypeNotRegistered) {
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
	wp.Start(&mockConn{id: 1}, writeFn, nil)

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

	blockFirst <- struct{}{}
	wp.Stop(nil)
}

func TestWriteProcessor_FlushTimeout(t *testing.T) {
	opts := kknet.WriteOptions{
		SendQueueSize:             4,
		SendQueueStrict:           false,
		SendQueueNeedFlushOver:    true,
		SendQueueTimeoutFlushOver: 50 * time.Millisecond,
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
		time.Sleep(100 * time.Millisecond)
		for i := 0; i < n; i++ {
			if batch[i] != nil {
				kkbuffer.Put(batch[i])
				batch[i] = nil
			}
		}
		return nil
	}
	conn := &mockConn{id: 1}
	wp.Start(conn, writeFn, nil)

	for i := 0; i < 2; i++ {
		bb := kkbuffer.GetWithCapacity(8)
		bb.B = bb.B[:8]
		if err := wp.SendBuffer(bb); err != nil {
			t.Fatalf("SendBuffer: %v", err)
		}
	}

	go wp.Stop(nil)

	select {
	case to := <-flushTimeoutCh:
		if to < 40*time.Millisecond {
			t.Errorf("flush timeout callback: got %v, want >= 40ms", to)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("flush timeout callback should be invoked")
	}
}
