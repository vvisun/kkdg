package kktcptls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
)

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func genTestTLSConfig(t *testing.T) *tls.Config {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "kktcptls_test"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}
	certDER, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("X509KeyPair: %v", err)
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}}
}

func TestServer_NewServer(t *testing.T) {
	tlsCfg := genTestTLSConfig(t)
	opts := kknet.ApplyOptions(kknet.WithTLSConfig(tlsCfg))
	s := NewServer("127.0.0.1:0", nil, opts)
	if s == nil {
		t.Fatal("NewServer returned nil")
	}
	if s.GetConnManager() == nil {
		t.Error("GetConnManager() returned nil")
	}
}

func TestServer_Start_Stop(t *testing.T) {
	addr := freePort(t)
	tlsCfg := genTestTLSConfig(t)
	opts := kknet.ApplyOptions(kknet.WithTLSConfig(tlsCfg))
	s := NewServer(addr, nil, opts)
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	stats := s.Stats()
	if stats.ActiveConns != 0 {
		t.Errorf("ActiveConns = %d, want 0", stats.ActiveConns)
	}
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := s.Stop(); err != kkerrors.ErrServerNotStarted {
		t.Errorf("second Stop() = %v, want ErrServerNotStarted", err)
	}
}

func TestServer_Stop_WithoutStart(t *testing.T) {
	tlsCfg := genTestTLSConfig(t)
	opts := kknet.ApplyOptions(kknet.WithTLSConfig(tlsCfg))
	s := NewServer("127.0.0.1:0", nil, opts)
	err := s.Stop()
	if err != kkerrors.ErrServerNotStarted {
		t.Errorf("Stop() = %v, want ErrServerNotStarted", err)
	}
}

func TestServer_Addr(t *testing.T) {
	addr := freePort(t)
	tlsCfg := genTestTLSConfig(t)
	opts := kknet.ApplyOptions(kknet.WithTLSConfig(tlsCfg))
	s := NewServer(addr, nil, opts)
	if s.Addr() != addr {
		t.Errorf("Addr() = %q, want %q", s.Addr(), addr)
	}
	_ = s.Start()
	defer s.Stop()
	if s.Addr() != addr {
		t.Errorf("Addr() after Start = %q, want %q", s.Addr(), addr)
	}
}
