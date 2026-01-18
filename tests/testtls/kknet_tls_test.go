package testtls

import (
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
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/kknet/kkws"
)

func tlsConfigPair(t *testing.T) (*tls.Config, *tls.Config) {
	t.Helper()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatalf("serial: %v", err)
	}

	template := x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: "127.0.0.1",
		},
		NotBefore: time.Now().Add(-time.Hour),
		NotAfter:  time.Now().Add(24 * time.Hour),
		KeyUsage:  x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth,
			x509.ExtKeyUsageClientAuth,
		},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("load cert: %v", err)
	}

	serverCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
	}
	clientCfg := &tls.Config{
		InsecureSkipVerify: true,
	}
	return serverCfg, clientCfg
}

func TestKKTcpTLS(t *testing.T) {
	serverTLS, clientTLS := tlsConfigPair(t)
	addr := freeTCPAddr(t)

	serverHandler := &testHandler{
		onMessage: func(c kknet.IConn, data []byte) {
			_ = c.Send(data)
		},
	}
	server := kktcp.NewServer(addr, serverHandler, kknet.WithTLSConfig(serverTLS))
	if err := server.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer func() { _ = server.Stop() }()

	replyCh := make(chan []byte, 1)
	clientHandler := &testHandler{
		onMessage: func(c kknet.IConn, data []byte) {
			replyCh <- data
		},
	}
	client := kktcp.NewClient(addr, clientHandler, kknet.WithTLSConfig(clientTLS))
	if err := client.Connect(); err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer func() { _ = client.Close() }()

	payload := []byte("tls")
	if err := client.Send(payload); err != nil {
		t.Fatalf("client send: %v", err)
	}

	select {
	case got := <-replyCh:
		if string(got) != string(payload) {
			t.Fatalf("unexpected reply: %s", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("tls reply timeout")
	}
}

func TestKKWSTLS(t *testing.T) {
	serverTLS, clientTLS := tlsConfigPair(t)
	addr := freeTCPAddr(t)

	serverHandler := &testHandler{
		onMessage: func(c kknet.IConn, data []byte) {
			_ = c.Send(data)
		},
	}
	server := kkws.NewServer(addr, serverHandler, kknet.WithTLSConfig(serverTLS))
	server.SetPath("/ws")

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Start()
	}()
	defer func() { _ = server.Stop() }()

	clientHandler := &testHandler{}
	client := kkws.NewClient("wss://"+addr+"/ws", clientHandler, kknet.WithTLSConfig(clientTLS))

	deadline := time.Now().Add(2 * time.Second)
	for {
		err := client.Connect()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("client connect: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	defer func() { _ = client.Close() }()

	replyCh := make(chan []byte, 1)
	clientHandler.onMessage = func(c kknet.IConn, data []byte) {
		replyCh <- data
	}

	payload := []byte("tls")
	if err := client.Send(payload); err != nil {
		t.Fatalf("client send: %v", err)
	}

	select {
	case got := <-replyCh:
		if string(got) != string(payload) {
			t.Fatalf("unexpected reply: %s", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("wss reply timeout")
	}
}
