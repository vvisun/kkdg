package kktcptls

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

type tlsRawRecvHandler struct {
	ch chan []byte
}

func (h *tlsRawRecvHandler) OnConnect(kknet.IConn)      {}
func (h *tlsRawRecvHandler) OnClose(kknet.IConn, error) {}
func (h *tlsRawRecvHandler) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	if data != nil {
		b := append([]byte(nil), data.Bytes()...)
		select {
		case h.ch <- b:
		default:
		}
	}
	kkbuffer.Put(data)
}

func TestClient_NewClient(t *testing.T) {
	tlsCfg := genTestTLSConfig(t)
	opts := kknet.ApplyOptions(kknet.WithTLSConfig(tlsCfg))
	c := NewClient("127.0.0.1:8080", nil, opts)
	if c == nil {
		t.Fatal("NewClient returned nil")
	}
	if c.Addr() != "127.0.0.1:8080" {
		t.Errorf("Addr() = %q, want 127.0.0.1:8080", c.Addr())
	}
}

type tlsNoopRawHandler struct{}

func (h *tlsNoopRawHandler) OnRaw(_ kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	kkbuffer.Put(data)
}

type tlsLifecycleHandler struct {
	onConnectCount atomic.Int32
	onCloseCount   atomic.Int32
	connectCh      chan struct{}
	closeCh        chan struct{}
}

func (h *tlsLifecycleHandler) OnConnect(kknet.IConn) {
	h.onConnectCount.Add(1)
	if h.connectCh != nil {
		select {
		case h.connectCh <- struct{}{}:
		default:
		}
	}
}

func (h *tlsLifecycleHandler) OnClose(kknet.IConn, error) {
	h.onCloseCount.Add(1)
	if h.closeCh != nil {
		select {
		case h.closeCh <- struct{}{}:
		default:
		}
	}
}

func TestClient_Connect_Close_TLS(t *testing.T) {
	addr := freePort(t)
	tlsCfg := genTestTLSConfig(t)
	noop := &tlsNoopRawHandler{}
	srvOpts := kknet.ApplyOptions(
		kknet.WithTLSConfig(tlsCfg),
		kknet.WithRawHandler(noop),
	)
	s := NewServer(addr, nil, srvOpts)
	if err := s.Start(); err != nil {
		t.Fatalf("Server Start: %v", err)
	}
	defer s.Stop()

	clientCfg := tlsCfg.Clone()
	clientCfg.InsecureSkipVerify = true
	cliOpts := kknet.ApplyOptions(
		kknet.WithTLSConfig(clientCfg),
		kknet.WithRawHandler(noop),
		kknet.WithIsNeedReconnect(false),
	)
	client := NewClient(addr, nil, cliOpts)
	if err := client.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if !client.IsConnected() {
		t.Error("IsConnected() should be true")
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := client.Close(); err != kkerrors.ErrNetClientNotConnected {
		t.Errorf("Close when not connected = %v, want ErrClientNotConnected", err)
	}
}

func TestClient_SendBuffer_Echo(t *testing.T) {
	addr := freePort(t)
	tlsCfg := genTestTLSConfig(t)
	recvCh := make(chan []byte, 16)
	echoHandler := &tlsEchoHandler{}
	clientRecv := &tlsRawRecvHandler{ch: recvCh}

	srvOpts := kknet.ApplyOptions(
		kknet.WithRawHandler(echoHandler),
		kknet.WithTLSConfig(tlsCfg),
	)
	s := NewServer(addr, echoHandler, srvOpts)
	echoHandler.server = s
	if err := s.Start(); err != nil {
		t.Fatalf("Server Start: %v", err)
	}
	defer s.Stop()

	clientCfg := tlsCfg.Clone()
	clientCfg.InsecureSkipVerify = true
	cliOpts := kknet.ApplyOptions(
		kknet.WithRawHandler(clientRecv),
		kknet.WithTLSConfig(clientCfg),
		kknet.WithIsNeedReconnect(false),
	)
	client := NewClient(addr, clientRecv, cliOpts)
	if err := client.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Close()

	time.Sleep(100 * time.Millisecond)

	payload := []byte("hello tls")
	bb, err := cliOpts.StreamTool.Pack(payload)
	if err != nil {
		t.Fatalf("Pack: %v", err)
	}
	if err := client.SendBuffer(bb); err != nil {
		t.Fatalf("SendBuffer: %v", err)
	}
	select {
	case got := <-recvCh:
		msg, err := cliOpts.StreamTool.Unpack(got)
		if err != nil {
			t.Fatalf("Unpack: %v", err)
		}
		if string(msg) != string(payload) {
			t.Errorf("got %q, want %q", msg, payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for echo")
	}
}

func TestClient_Connect_Close_NoTLS(t *testing.T) {
	addr := freePort(t)
	noop := &tlsNoopRawHandler{}

	srvOpts := kknet.ApplyOptions(
		kknet.WithRawHandler(noop),
	)
	s := NewServer(addr, nil, srvOpts)
	if err := s.Start(); err != nil {
		t.Fatalf("Server Start: %v", err)
	}
	defer s.Stop()

	cliOpts := kknet.ApplyOptions(
		kknet.WithRawHandler(noop),
		kknet.WithIsNeedReconnect(false),
	)
	client := NewClient(addr, nil, cliOpts)
	if err := client.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if !client.IsConnected() {
		t.Error("IsConnected() should be true")
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := client.Close(); err != kkerrors.ErrNetClientNotConnected {
		t.Errorf("Close when not connected = %v, want ErrClientNotConnected", err)
	}
}

func TestClient_SendBuffer_Echo_NoTLS(t *testing.T) {
	addr := freePort(t)
	recvCh := make(chan []byte, 16)
	echoHandler := &tlsEchoHandler{}
	clientRecv := &tlsRawRecvHandler{ch: recvCh}

	srvOpts := kknet.ApplyOptions(
		kknet.WithRawHandler(echoHandler),
	)
	s := NewServer(addr, echoHandler, srvOpts)
	echoHandler.server = s
	if err := s.Start(); err != nil {
		t.Fatalf("Server Start: %v", err)
	}
	defer s.Stop()

	cliOpts := kknet.ApplyOptions(
		kknet.WithRawHandler(clientRecv),
		kknet.WithIsNeedReconnect(false),
	)
	client := NewClient(addr, clientRecv, cliOpts)
	if err := client.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Close()

	time.Sleep(100 * time.Millisecond)

	payload := []byte("hello plain tcp")
	bb, err := cliOpts.StreamTool.Pack(payload)
	if err != nil {
		t.Fatalf("Pack: %v", err)
	}
	if err := client.SendBuffer(bb); err != nil {
		t.Fatalf("SendBuffer: %v", err)
	}
	select {
	case got := <-recvCh:
		msg, err := cliOpts.StreamTool.Unpack(got)
		if err != nil {
			t.Fatalf("Unpack: %v", err)
		}
		if string(msg) != string(payload) {
			t.Errorf("got %q, want %q", msg, payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for echo")
	}
}

func TestClient_Reconnect_NoTLS_ServerRestart(t *testing.T) {
	addr := freePort(t)
	recvCh := make(chan []byte, 16)
	echoHandler := &tlsEchoHandler{}
	clientRecv := &tlsRawRecvHandler{ch: recvCh}
	lh := &tlsLifecycleHandler{
		connectCh: make(chan struct{}, 8),
		closeCh:   make(chan struct{}, 8),
	}

	srvOpts := kknet.ApplyOptions(
		kknet.WithRawHandler(echoHandler),
	)
	s := NewServer(addr, echoHandler, srvOpts)
	echoHandler.server = s
	if err := s.Start(); err != nil {
		t.Fatalf("Server Start: %v", err)
	}
	defer s.Stop()

	cliOpts := kknet.ApplyOptions(
		kknet.WithRawHandler(clientRecv),
		kknet.WithIsNeedReconnect(true),
		kknet.WithReconnectInterval(500*time.Millisecond, 0),
		kknet.WithReconnectMaxInterval(500*time.Millisecond),
	)
	client := NewClient(addr, lh, cliOpts)
	if err := client.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Close()

	// baseline round trip
	firstPayload := []byte("first-round")
	bb, err := cliOpts.StreamTool.Pack(firstPayload)
	if err != nil {
		t.Fatalf("Pack first: %v", err)
	}
	if err := client.SendBuffer(bb); err != nil {
		t.Fatalf("SendBuffer first: %v", err)
	}
	select {
	case got := <-recvCh:
		msg, err := cliOpts.StreamTool.Unpack(got)
		if err != nil {
			t.Fatalf("Unpack first: %v", err)
		}
		if string(msg) != string(firstPayload) {
			t.Fatalf("first got %q, want %q", msg, firstPayload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting first echo")
	}

	// stop server and wait client OnClose
	if err := s.Stop(); err != nil {
		t.Fatalf("Server Stop: %v", err)
	}
	select {
	case <-lh.closeCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting client OnClose after server stop")
	}

	// restart server on same addr; client should reconnect automatically
	s = NewServer(addr, echoHandler, srvOpts)
	echoHandler.server = s
	if err := s.Start(); err != nil {
		t.Fatalf("Server Restart Start: %v", err)
	}
	defer s.Stop()

	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if client.IsConnected() && lh.onConnectCount.Load() >= 2 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !client.IsConnected() {
		t.Fatal("client should reconnect after server restart")
	}
	if lh.onConnectCount.Load() < 2 {
		t.Fatalf("expected OnConnect >= 2, got %d", lh.onConnectCount.Load())
	}

	// round trip after reconnect
	secondPayload := []byte("second-round")
	bb2, err := cliOpts.StreamTool.Pack(secondPayload)
	if err != nil {
		t.Fatalf("Pack second: %v", err)
	}
	if err := client.SendBuffer(bb2); err != nil {
		t.Fatalf("SendBuffer second: %v", err)
	}
	select {
	case got := <-recvCh:
		msg, err := cliOpts.StreamTool.Unpack(got)
		if err != nil {
			t.Fatalf("Unpack second: %v", err)
		}
		if string(msg) != string(secondPayload) {
			t.Fatalf("second got %q, want %q", msg, secondPayload)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting second echo")
	}
}

func TestClient_ReconnectMaxRetries_StatusClosed(t *testing.T) {
	noop := &tlsNoopRawHandler{}
	// reserve a local free port without starting a server, forcing fast ECONNREFUSED
	addr := freePort(t)
	opts := kknet.ApplyOptions(
		kknet.WithRawHandler(noop),
		kknet.WithIsNeedReconnect(true),
		kknet.WithReconnectInterval(100*time.Millisecond, 1),
		kknet.WithReconnectMaxInterval(100*time.Millisecond),
	)
	client := NewClient(addr, nil, opts)

	err := client.Connect()
	if err == nil {
		t.Fatal("Connect() should fail when target is unreachable")
	}
	if client.IsConnected() {
		t.Fatal("IsConnected() should be false after reconnect attempts exhausted")
	}
	if !client.IsStopped() {
		t.Fatal("IsStopped() should be true after reconnect attempts exhausted")
	}
}

func TestClient_Close_Concurrent_NoPanic(t *testing.T) {
	addr := freePort(t)
	tlsCfg := genTestTLSConfig(t)
	noop := &tlsNoopRawHandler{}
	srvOpts := kknet.ApplyOptions(
		kknet.WithTLSConfig(tlsCfg),
		kknet.WithRawHandler(noop),
	)
	s := NewServer(addr, nil, srvOpts)
	if err := s.Start(); err != nil {
		t.Fatalf("Server Start: %v", err)
	}
	defer s.Stop()

	clientCfg := tlsCfg.Clone()
	clientCfg.InsecureSkipVerify = true
	cliOpts := kknet.ApplyOptions(
		kknet.WithTLSConfig(clientCfg),
		kknet.WithRawHandler(noop),
		kknet.WithIsNeedReconnect(false),
	)
	client := NewClient(addr, nil, cliOpts)
	if err := client.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- client.Close()
		}()
	}
	wg.Wait()
	close(errs)

	var hasNil, hasNotConnected bool
	for err := range errs {
		if err == nil {
			hasNil = true
			continue
		}
		if err == kkerrors.ErrNetClientNotConnected {
			hasNotConnected = true
			continue
		}
		t.Fatalf("unexpected Close error: %v", err)
	}
	if !hasNil {
		t.Fatal("expected one successful Close()")
	}
	if !hasNotConnected {
		t.Fatal("expected one ErrNetClientNotConnected on concurrent Close()")
	}
}

type tlsEchoHandler struct {
	server kknet.IServer
}

func (h *tlsEchoHandler) OnConnect(c kknet.IConn)          {}
func (h *tlsEchoHandler) OnClose(c kknet.IConn, err error) {}
func (h *tlsEchoHandler) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	if data == nil || h.server == nil {
		if data != nil {
			kkbuffer.Put(data)
		}
		return
	}
	bb := kkbuffer.GetWithCapacity(len(data.Bytes()))
	bb.B = append(bb.B[:0], data.Bytes()...)
	kkbuffer.Put(data)
	_ = h.server.SendBuffer(connID, bb)
}
