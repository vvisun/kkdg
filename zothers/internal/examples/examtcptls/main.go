package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kktcptls"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

// examtcptls 演示 TCP TLS 服务端与客户端的基本用法（Echo 回显）
// cd zothers/internal/examples/examtcptls && go run .

func main() {
	addr := freePort()
	tlsConfig := genSelfSignedTLSConfig()
	runEchoDemo(addr, tlsConfig)
}

func freePort() string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	a := ln.Addr().String()
	_ = ln.Close()
	return a
}

func genSelfSignedTLSConfig() *tls.Config {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(err)
	}
	tpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "examtcptls"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}
	certDER, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		panic(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		panic(err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
	}
}

func runEchoDemo(addr string, tlsCfg *tls.Config) {
	recvCh := make(chan []byte, 4)
	echoHandler := &tlsEchoHandler{}

	streamTool := kkpacket.DefaultStreamPacket()

	opts := kknet.ApplyOptions(
		kknet.WithStreamTool(streamTool),
		kknet.WithRawHandler(echoHandler),
		kknet.WithLogger(kklog.GetConsoleLogger()),
		kknet.WithTLSConfig(tlsCfg),
	)

	srv := kktcptls.NewServer(addr, echoHandler, opts)
	echoHandler.server = srv

	if err := srv.Start(); err != nil {
		panic(err)
	}
	defer srv.Stop()

	kklog.Infof("[examtcptls] tls server listening on %s", addr)

	// 客户端需 InsecureSkipVerify 以接受自签名证书
	clientCfg := tlsCfg.Clone()
	clientCfg.InsecureSkipVerify = true

	clientHandler := &tlsRecvHandler{ch: recvCh}

	client := kktcptls.NewClient(addr, clientHandler, kknet.ApplyOptions(
		kknet.WithRawHandler(clientHandler),
		kknet.WithLogger(kklog.GetConsoleLogger()),
		kknet.WithIsNeedReconnect(false),
		kknet.WithTLSConfig(clientCfg),
	))

	if err := client.Connect(); err != nil {
		panic(err)
	}
	defer client.Close()

	time.Sleep(100 * time.Millisecond)

	payload := []byte("hello examtcptls")
	bb, err := streamTool.Pack(payload)
	if err != nil {
		panic(err)
	}
	if err := client.SendBuffer(bb); err != nil {
		panic(err)
	}

	select {
	case got := <-recvCh:
		msg, err := streamTool.Unpack(got)
		if err != nil {
			panic(err)
		}
		fmt.Printf("received echo: %q\n", string(msg))
		if string(msg) != string(payload) {
			panic("echo mismatch")
		}
	case <-time.After(2 * time.Second):
		panic("timeout waiting for echo")
	}

	fmt.Println("examtcptls demo ok")
}

type tlsEchoHandler struct {
	server kknet.IServer
}

func (h *tlsEchoHandler) OnConnect(c kknet.IConn) {
	kklog.Infof("[examtcptls] client connected: connID=%d", c.ID())
}

func (h *tlsEchoHandler) OnClose(c kknet.IConn, err error) {
	kklog.Infof("[examtcptls] client closed: connID=%d", c.ID())
}

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
	if err := h.server.SendBuffer(connID, bb); err != nil {
		kklog.Errorf("[examtcptls] send error: %v", err)
		kkbuffer.Put(bb)
	}
}

type tlsRecvHandler struct {
	ch chan []byte
}

func (h *tlsRecvHandler) OnConnect(c kknet.IConn)          {}
func (h *tlsRecvHandler) OnClose(c kknet.IConn, err error) {}

func (h *tlsRecvHandler) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	if data == nil || h.ch == nil {
		if data != nil {
			kkbuffer.Put(data)
		}
		return
	}
	b := append([]byte(nil), data.Bytes()...)
	kkbuffer.Put(data)
	select {
	case h.ch <- b:
	default:
	}
}
