package kktcp

import (
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/panjf2000/gnet/v2"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/queues/bbqueue"
)

type fakeAddr string

func (a fakeAddr) Network() string { return "tcp" }
func (a fakeAddr) String() string  { return string(a) }

type fakeGnetConn struct {
	mu sync.Mutex

	closed  bool
	ctx     any
	inbound []byte

	asyncWritevHook func(bs [][]byte, callback gnet.AsyncCallback) error
}

func (c *fakeGnetConn) Read(_ []byte) (int, error)          { return 0, io.EOF }
func (c *fakeGnetConn) Write(_ []byte) (int, error)         { return 0, nil }
func (c *fakeGnetConn) WriteTo(_ io.Writer) (int64, error)  { return 0, nil }
func (c *fakeGnetConn) ReadFrom(_ io.Reader) (int64, error) { return 0, nil }
func (c *fakeGnetConn) Next(n int) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n > len(c.inbound) {
		return nil, io.ErrShortBuffer
	}
	data := append([]byte(nil), c.inbound[:n]...)
	c.inbound = c.inbound[n:]
	return data, nil
}
func (c *fakeGnetConn) Peek(n int) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n > len(c.inbound) {
		return nil, io.ErrShortBuffer
	}
	return append([]byte(nil), c.inbound[:n]...), nil
}
func (c *fakeGnetConn) Discard(n int) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n > len(c.inbound) {
		n = len(c.inbound)
	}
	c.inbound = c.inbound[n:]
	return n, nil
}
func (c *fakeGnetConn) InboundBuffered() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.inbound)
}
func (c *fakeGnetConn) SendTo(_ []byte, _ net.Addr) (int, error) { return 0, nil }
func (c *fakeGnetConn) Writev(_ [][]byte) (int, error)           { return 0, nil }
func (c *fakeGnetConn) Flush() error                             { return nil }
func (c *fakeGnetConn) OutboundBuffered() int                    { return 0 }
func (c *fakeGnetConn) AsyncWrite(_ []byte, callback gnet.AsyncCallback) error {
	if callback != nil {
		return callback(c, nil)
	}
	return nil
}
func (c *fakeGnetConn) AsyncWritev(bs [][]byte, callback gnet.AsyncCallback) error {
	if c.asyncWritevHook != nil {
		return c.asyncWritevHook(bs, callback)
	}
	if callback != nil {
		return callback(c, nil)
	}
	return nil
}
func (c *fakeGnetConn) Context() any              { return c.ctx }
func (c *fakeGnetConn) EventLoop() gnet.EventLoop { return nil }
func (c *fakeGnetConn) SetContext(v any)          { c.ctx = v }
func (c *fakeGnetConn) LocalAddr() net.Addr       { return fakeAddr("local") }
func (c *fakeGnetConn) RemoteAddr() net.Addr      { return fakeAddr("remote") }
func (c *fakeGnetConn) Wake(callback gnet.AsyncCallback) error {
	if callback != nil {
		return callback(c, nil)
	}
	return nil
}
func (c *fakeGnetConn) CloseWithCallback(callback gnet.AsyncCallback) error {
	if err := c.Close(); err != nil {
		return err
	}
	if callback != nil {
		return callback(c, nil)
	}
	return nil
}
func (c *fakeGnetConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return nil
}
func (c *fakeGnetConn) SetDeadline(_ time.Time) error                        { return nil }
func (c *fakeGnetConn) SetReadDeadline(_ time.Time) error                    { return nil }
func (c *fakeGnetConn) SetWriteDeadline(_ time.Time) error                   { return nil }
func (c *fakeGnetConn) Fd() int                                              { return 0 }
func (c *fakeGnetConn) Dup() (int, error)                                    { return 0, nil }
func (c *fakeGnetConn) SetReadBuffer(_ int) error                            { return nil }
func (c *fakeGnetConn) SetWriteBuffer(_ int) error                           { return nil }
func (c *fakeGnetConn) SetLinger(_ int) error                                { return nil }
func (c *fakeGnetConn) SetKeepAlivePeriod(_ time.Duration) error             { return nil }
func (c *fakeGnetConn) SetKeepAlive(_ bool, _, _ time.Duration, _ int) error { return nil }
func (c *fakeGnetConn) SetNoDelay(_ bool) error                              { return nil }

func (c *fakeGnetConn) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *fakeGnetConn) setInbound(data []byte) {
	c.mu.Lock()
	c.inbound = append([]byte(nil), data...)
	c.mu.Unlock()
}

func newTestClientConn(t *testing.T, conn gnet.Conn) (*gnetConn, kknet.Options) {
	t.Helper()
	opts := kknet.ApplyOptions(kknet.WithSendQueueTimeoutFlushOver(200 * time.Millisecond))
	return &gnetConn{
		id:          1,
		conn:        conn,
		opts:        &opts,
		readStopped: make(chan struct{}),
		closeCond:   sync.NewCond(&sync.Mutex{}),
		sendQueue:   bbqueue.NewBBQueue(opts.WpOptions.SendQueueSize, opts.WpOptions.SendQueueStrict),
	}, opts
}

type blockingReadProcessor struct {
	stopEntered chan struct{}
	releaseStop chan struct{}
	stopOnce    sync.Once
}

func (rp *blockingReadProcessor) Start(kknet.IConn)          {}
func (rp *blockingReadProcessor) EnqueuePacket([]byte) error { return nil }
func (rp *blockingReadProcessor) OnRecvBytes([]byte) error   { return nil }
func (rp *blockingReadProcessor) Pending() int               { return 0 }
func (rp *blockingReadProcessor) Stop() {
	rp.stopOnce.Do(func() { close(rp.stopEntered) })
	<-rp.releaseStop
}

type enqueueErrReadProcessor struct {
	err error
}

func (rp *enqueueErrReadProcessor) Start(kknet.IConn)          {}
func (rp *enqueueErrReadProcessor) EnqueuePacket([]byte) error { return rp.err }
func (rp *enqueueErrReadProcessor) OnRecvBytes([]byte) error   { return nil }
func (rp *enqueueErrReadProcessor) Pending() int               { return 0 }
func (rp *enqueueErrReadProcessor) Stop()                      {}

type connTestLifecycleHandler struct {
	onClose func(kknet.IConn, error)
}

func (h *connTestLifecycleHandler) OnConnect(kknet.IConn) {}
func (h *connTestLifecycleHandler) OnClose(c kknet.IConn, err error) {
	if h.onClose != nil {
		h.onClose(c, err)
	}
}

func TestClientConn_Close_WaitsForQueueFlush(t *testing.T) {
	releaseCh := make(chan struct{})
	fc := &fakeGnetConn{}
	fc.asyncWritevHook = func(bs [][]byte, callback gnet.AsyncCallback) error {
		go func() {
			<-releaseCh
			if callback != nil {
				_ = callback(fc, nil)
			}
		}()
		return nil
	}
	conn, opts := newTestClientConn(t, fc)
	conn.closeCond = sync.NewCond(&conn.closeMu)

	bb, err := opts.StreamTool.Pack([]byte("hello"))
	if err != nil {
		t.Fatalf("Pack: %v", err)
	}
	if err := conn.SendBuffer(bb); err != nil {
		t.Fatalf("SendBuffer: %v", err)
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- conn.Close()
	}()

	select {
	case err := <-closeDone:
		t.Fatalf("Close returned early: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseCh)

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not return after flush callback")
	}
}

func TestClientConn_WriteError_ClosesAndDropsQueue(t *testing.T) {
	writeErr := errors.New("async write failed")
	releaseCh := make(chan struct{})
	fc := &fakeGnetConn{}
	fc.asyncWritevHook = func(bs [][]byte, callback gnet.AsyncCallback) error {
		go func() {
			<-releaseCh
			if callback != nil {
				_ = callback(fc, writeErr)
			}
		}()
		return nil
	}
	conn, opts := newTestClientConn(t, fc)
	conn.closeCond = sync.NewCond(&conn.closeMu)

	bb1, err := opts.StreamTool.Pack([]byte("first"))
	if err != nil {
		t.Fatalf("Pack first: %v", err)
	}
	if err := conn.SendBuffer(bb1); err != nil {
		t.Fatalf("SendBuffer first: %v", err)
	}
	bb2, err := opts.StreamTool.Pack([]byte("second"))
	if err != nil {
		t.Fatalf("Pack second: %v", err)
	}
	if err := conn.SendBuffer(bb2); err != nil {
		t.Fatalf("SendBuffer second: %v", err)
	}

	close(releaseCh)
	time.Sleep(50 * time.Millisecond)

	if !conn.closing.Load() {
		t.Fatal("closing should be true after write error")
	}
	if got := conn.sendQueue.Len(); got != 0 {
		t.Fatalf("sendQueue.Len() = %d, want 0", got)
	}
	if !fc.isClosed() {
		t.Fatal("underlying conn should be closed after write error")
	}

	bb3, err := opts.StreamTool.Pack([]byte("late"))
	if err != nil {
		t.Fatalf("Pack late: %v", err)
	}
	if err := conn.SendBuffer(bb3); err != kkerrors.ErrNetConnectionClosed {
		t.Fatalf("SendBuffer after write error = %v, want ErrNetConnectionClosed", err)
	}
}

func TestClientConn_Close_FlushTimeout_DropsQueue(t *testing.T) {
	fc := &fakeGnetConn{}
	fc.asyncWritevHook = func(_ [][]byte, _ gnet.AsyncCallback) error {
		return nil
	}
	conn, opts := newTestClientConn(t, fc)
	conn.closeCond = sync.NewCond(&conn.closeMu)
	opts.WpOptions.SendQueueNeedFlushOver = true
	opts.WpOptions.SendQueueTimeoutFlushOver = 200 * time.Millisecond

	bb1, err := opts.StreamTool.Pack([]byte("first"))
	if err != nil {
		t.Fatalf("Pack first: %v", err)
	}
	if err := conn.SendBuffer(bb1); err != nil {
		t.Fatalf("SendBuffer first: %v", err)
	}
	bb2, err := opts.StreamTool.Pack([]byte("second"))
	if err != nil {
		t.Fatalf("Pack second: %v", err)
	}
	if err := conn.SendBuffer(bb2); err != nil {
		t.Fatalf("SendBuffer second: %v", err)
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if got := conn.sendQueue.Len(); got != 0 {
		t.Fatalf("sendQueue.Len() = %d, want 0 after flush timeout", got)
	}
	if !fc.isClosed() {
		t.Fatal("underlying conn should be closed after flush timeout")
	}
}

func TestClientHandler_OnClose_DropsQueuedBuffers(t *testing.T) {
	fc := &fakeGnetConn{}
	fc.asyncWritevHook = func(_ [][]byte, _ gnet.AsyncCallback) error {
		return nil
	}
	conn, opts := newTestClientConn(t, fc)
	conn.closeCond = sync.NewCond(&conn.closeMu)
	fc.SetContext(conn)

	bb1, err := opts.StreamTool.Pack([]byte("first"))
	if err != nil {
		t.Fatalf("Pack first: %v", err)
	}
	if err := conn.SendBuffer(bb1); err != nil {
		t.Fatalf("SendBuffer first: %v", err)
	}
	bb2, err := opts.StreamTool.Pack([]byte("second"))
	if err != nil {
		t.Fatalf("Pack second: %v", err)
	}
	if err := conn.SendBuffer(bb2); err != nil {
		t.Fatalf("SendBuffer second: %v", err)
	}

	handler := &gnetClientEventHandler{client: &GnetClient{opts: opts}}
	handler.OnClose(fc, errors.New("peer closed"))

	if !conn.closing.Load() {
		t.Fatal("closing should be true after OnClose")
	}
	if got := conn.sendQueue.Len(); got != 0 {
		t.Fatalf("sendQueue.Len() = %d, want 0 after OnClose", got)
	}
}

func TestClientConn_Close_WaitsForReadProcessorStop(t *testing.T) {
	fc := &fakeGnetConn{}
	conn, _ := newTestClientConn(t, fc)
	conn.closeCond = sync.NewCond(&conn.closeMu)
	rp := &blockingReadProcessor{
		stopEntered: make(chan struct{}),
		releaseStop: make(chan struct{}),
	}
	conn.rp = rp

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- conn.Close()
	}()

	select {
	case <-rp.stopEntered:
	case <-time.After(time.Second):
		t.Fatal("read processor Stop was not called")
	}

	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before rp.Stop finished: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(rp.releaseStop)

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not return after rp.Stop finished")
	}
}

func TestClientHandler_OnClose_WaitsForReadProcessorStopBeforeCallback(t *testing.T) {
	fc := &fakeGnetConn{}
	conn, opts := newTestClientConn(t, fc)
	conn.closeCond = sync.NewCond(&conn.closeMu)
	rp := &blockingReadProcessor{
		stopEntered: make(chan struct{}),
		releaseStop: make(chan struct{}),
	}
	conn.rp = rp
	fc.SetContext(conn)

	closedCh := make(chan error, 1)
	handler := &gnetClientEventHandler{
		client: &GnetClient{
			opts: opts,
			handler: &connTestLifecycleHandler{
				onClose: func(_ kknet.IConn, err error) {
					closedCh <- err
				},
			},
		},
	}

	closeErr := errors.New("peer closed")
	handler.OnClose(fc, closeErr)

	select {
	case <-rp.stopEntered:
	case <-time.After(time.Second):
		t.Fatal("read processor Stop was not called")
	}

	select {
	case err := <-closedCh:
		t.Fatalf("OnClose callback fired before rp.Stop finished: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(rp.releaseStop)

	select {
	case err := <-closedCh:
		if !errors.Is(err, closeErr) {
			t.Fatalf("OnClose err = %v, want %v", err, closeErr)
		}
	case <-time.After(time.Second):
		t.Fatal("OnClose callback did not fire after rp.Stop finished")
	}
}

func TestClientHandler_OnTraffic_RecvQueueFull_Closes(t *testing.T) {
	fc := &fakeGnetConn{}
	opts := kknet.ApplyOptions()
	bb, err := opts.StreamTool.Pack([]byte("hello"))
	if err != nil {
		t.Fatalf("Pack: %v", err)
	}
	defer kkbuffer.Put(bb)
	fc.setInbound(bb.B)

	conn, _ := newTestClientConn(t, fc)
	conn.rp = &enqueueErrReadProcessor{err: kkerrors.ErrNetRecvQueueFull}
	fc.SetContext(conn)

	client := &GnetClient{}
	handler := &gnetClientEventHandler{client: client}
	action := handler.OnTraffic(fc)

	if action != gnet.Close {
		t.Fatalf("OnTraffic action = %v, want %v", action, gnet.Close)
	}
	if got := client.stats.Snapshot().Errors; got != 1 {
		t.Fatalf("stats.Errors = %d, want 1", got)
	}
}

func TestServerHandler_OnTraffic_RecvQueueFull_Closes(t *testing.T) {
	fc := &fakeGnetConn{}
	opts := kknet.ApplyOptions()
	bb, err := opts.StreamTool.Pack([]byte("hello"))
	if err != nil {
		t.Fatalf("Pack: %v", err)
	}
	defer kkbuffer.Put(bb)
	fc.setInbound(bb.B)

	conn, _ := newTestClientConn(t, fc)
	conn.rp = &enqueueErrReadProcessor{err: kkerrors.ErrNetRecvQueueFull}
	fc.SetContext(conn)

	server := &Server{}
	handler := &tcpEventHandler{server: server}
	action := handler.OnTraffic(fc)

	if action != gnet.Close {
		t.Fatalf("OnTraffic action = %v, want %v", action, gnet.Close)
	}
	if got := server.stats.Snapshot().Errors; got != 1 {
		t.Fatalf("stats.Errors = %d, want 1", got)
	}
}
