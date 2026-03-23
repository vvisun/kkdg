package main

import (
	"fmt"
	"net"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

// examtcp 演示 TCP 服务端与客户端的基本用法（Echo 回显）
// cd zothers/examples/examtcp; go run main.go

func main() {
	addr := freePort()
	runEchoDemo(addr)
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

func runEchoDemo(addr string) {
	recvCh := make(chan []byte, 4)
	echoHandler := &tcpEchoHandler{}
	streamTool := kkpacket.DefaultStreamPacket()

	opts := kknet.ApplyOptions(
		kknet.WithStreamTool(streamTool),
		kknet.WithRawHandler(echoHandler),
		kknet.WithLogger(kklog.GetConsoleLogger()),
	)

	srv := kktcp.NewServer(addr, echoHandler, opts)
	echoHandler.server = srv

	if err := srv.Start(); err != nil {
		panic(err)
	}
	defer srv.Stop()

	kklog.Infof("[examtcp] server listening on %s", addr)

	clientHandler := &tcpRecvHandler{ch: recvCh}

	client := kktcp.NewClient(addr, clientHandler, kknet.ApplyOptions(
		kknet.WithRawHandler(clientHandler),
		kknet.WithLogger(kklog.GetConsoleLogger()),
		kknet.WithIsNeedReconnect(false),
	))

	if err := client.Connect(); err != nil {
		panic(err)
	}
	defer client.Close()

	time.Sleep(100 * time.Millisecond)

	payload := []byte("hello examtcp")
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

	fmt.Println("examtcp demo ok")
}

type tcpEchoHandler struct {
	server kknet.IServer
}

func (h *tcpEchoHandler) OnConnect(c kknet.IConn) {
	kklog.Infof("[examtcp] client connected: connID=%d", c.ID())
}

func (h *tcpEchoHandler) OnClose(c kknet.IConn, err error) {
	kklog.Infof("[examtcp] client closed: connID=%d", c.ID())
}

func (h *tcpEchoHandler) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
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
		kklog.Errorf("[examtcp] send error: %v", err)
		kkbuffer.Put(bb)
	}
}

type tcpRecvHandler struct {
	ch chan []byte
}

func (h *tcpRecvHandler) OnConnect(c kknet.IConn)          {}
func (h *tcpRecvHandler) OnClose(c kknet.IConn, err error) {}

func (h *tcpRecvHandler) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
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
