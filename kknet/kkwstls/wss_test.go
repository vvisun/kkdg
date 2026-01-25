package kkwstls

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers"
)

type echoHandler struct{}

func (h *echoHandler) OnConnect(_ kknet.IConn) {}

func (h *echoHandler) OnMessage(c kknet.IConn, data buffers.IBuffer) {
	_ = c.Send(data.B)
}

func (h *echoHandler) OnClose(_ kknet.IConn, _ error) {}

type clientCaptureHandler struct {
	connectCh chan struct{}
	msgCh     chan []byte
	closeCh   chan error
}

func (h *clientCaptureHandler) OnConnect(_ kknet.IConn) {
	select {
	case h.connectCh <- struct{}{}:
	default:
	}
}

func (h *clientCaptureHandler) OnMessage(_ kknet.IConn, data buffers.IBuffer) {
	msg := make([]byte, len(data.B))
	copy(msg, data.B)
	h.msgCh <- msg
}

func (h *clientCaptureHandler) OnClose(_ kknet.IConn, err error) {
	select {
	case h.closeCh <- err:
	default:
	}
}

func TestWSSConnectAndEcho(t *testing.T) {
	cert, pool := generateSelfSignedCert(t)
	server := NewServer("127.0.0.1:0", &echoHandler{}, kknet.WithTLSConfig(&tls.Config{
		Certificates: []tls.Certificate{cert},
	}))
	server.SetPath("/ws")
	if err := server.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(func() { _ = server.Stop() })

	addr := server.tlsListener.Addr().String()
	clientHandler := &clientCaptureHandler{
		connectCh: make(chan struct{}, 1),
		msgCh:     make(chan []byte, 1),
		closeCh:   make(chan error, 1),
	}
	client := NewClient("wss://"+addr+"/ws", clientHandler, kknet.WithTLSConfig(&tls.Config{
		RootCAs: pool,
	}))
	if err := client.Connect(); err != nil {
		t.Fatalf("connect client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	select {
	case <-clientHandler.connectCh:
	case <-time.After(3 * time.Second):
		t.Fatalf("connect timeout")
	}

	payload := []byte("hello-wss")
	if err := client.Send(payload); err != nil {
		t.Fatalf("send: %v", err)
	}

	select {
	case got := <-clientHandler.msgCh:
		if !bytes.Equal(got, payload) {
			t.Fatalf("unexpected payload: %q != %q", got, payload)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("echo timeout")
	}
}

func TestWSSRequiresTLSConfig(t *testing.T) {
	server := NewServer("127.0.0.1:0", &echoHandler{})
	server.SetPath("/ws")
	if err := server.Start(); err == nil {
		t.Fatalf("expected error for missing TLSConfig")
	}

	client := NewClient("ws://127.0.0.1:8443/ws", &clientCaptureHandler{
		connectCh: make(chan struct{}, 1),
		msgCh:     make(chan []byte, 1),
		closeCh:   make(chan error, 1),
	})
	if err := client.Connect(); err == nil {
		t.Fatalf("expected error for non-wss without TLSConfig")
	}
}

func TestWSSWrongCA(t *testing.T) {
	serverCert, _ := generateSelfSignedCert(t)
	_, wrongPool := generateSelfSignedCert(t)
	server := NewServer("127.0.0.1:0", &echoHandler{}, kknet.WithTLSConfig(&tls.Config{
		Certificates: []tls.Certificate{serverCert},
	}))
	server.SetPath("/ws")
	if err := server.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(func() { _ = server.Stop() })

	addr := server.tlsListener.Addr().String()
	client := NewClient("wss://"+addr+"/ws", &clientCaptureHandler{
		connectCh: make(chan struct{}, 1),
		msgCh:     make(chan []byte, 1),
		closeCh:   make(chan error, 1),
	}, kknet.WithTLSConfig(&tls.Config{
		RootCAs: wrongPool,
	}))
	if err := client.Connect(); err == nil {
		t.Fatalf("expected error for wrong CA")
	}
}

func generateSelfSignedCert(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatalf("generate serial: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: "localhost",
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("load key pair: %v", err)
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(certPEM) {
		t.Fatalf("append cert to pool")
	}

	return cert, pool
}
