package kktcptls

import (
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
