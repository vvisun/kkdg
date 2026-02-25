package main

import (
	"fmt"
	"net"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kkudp"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

// examudp 演示 UDP 服务端与客户端的基本用法（Echo 回显）
// cd other/examples/examudp; go run main.go

func main() {
	addr := freeUDPPort()
	runEchoDemo(addr)
}

func freeUDPPort() string {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	addr := pc.LocalAddr().String()
	_ = pc.Close()
	return addr
}

func runEchoDemo(addr string) {
	recvCh := make(chan []byte, 4)
	echoHandler := &udpEchoHandler{}

	opts := kknet.ApplyOptions(
		kknet.WithRawHandler(echoHandler),
		kknet.WithLogger(kklog.GetConsoleLogger()),
	)

	srv := kkudp.NewServer(addr, echoHandler, opts)
	echoHandler.server = srv

	if err := srv.Start(); err != nil {
		panic(err)
	}
	defer srv.Stop()

	kklog.Infof("[examudp] server listening on %s", addr)

	clientHandler := &udpRecvHandler{ch: recvCh}

	client := kkudp.NewClient(addr, clientHandler, kknet.ApplyOptions(
		kknet.WithRawHandler(clientHandler),
		kknet.WithLogger(kklog.GetConsoleLogger()),
	))

	if err := client.Connect(); err != nil {
		panic(err)
	}
	defer client.Close()

	time.Sleep(100 * time.Millisecond)

	payload := []byte("hello examudp")
	bb, err := kkpacket.DefaultStreamPacket().Pack(payload)
	if err != nil {
		panic(err)
	}
	if err := client.SendBuffer(bb); err != nil {
		panic(err)
	}

	select {
	case got := <-recvCh:
		msg, err := kkpacket.DefaultStreamPacket().Unpack(got)
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

	fmt.Println("examudp demo ok")
}

type udpEchoHandler struct {
	server kknet.IServer
}

func (h *udpEchoHandler) OnConnect(c kknet.IConn) {
	kklog.Infof("[examudp] client connected: connID=%d", c.ID())
}

func (h *udpEchoHandler) OnClose(c kknet.IConn, err error) {
	kklog.Infof("[examudp] client closed: connID=%d", c.ID())
}

func (h *udpEchoHandler) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
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
		kklog.Errorf("[examudp] send error: %v", err)
		kkbuffer.Put(bb)
	}
}

type udpRecvHandler struct {
	ch chan []byte
}

func (h *udpRecvHandler) OnConnect(c kknet.IConn)          {}
func (h *udpRecvHandler) OnClose(c kknet.IConn, err error) {}

func (h *udpRecvHandler) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
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
