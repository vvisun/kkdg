package main

import (
	"fmt"
	"net"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kkws"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

// examws 演示 WebSocket 服务端与客户端的基本用法（Echo 回显）
// cd other/examples/examws; go run main.go

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
	echoHandler := &wsEchoHandler{}

	streamTool := kkpacket.DefaultStreamPacket()

	opts := kknet.ApplyOptions(
		kknet.WithRawHandler(echoHandler),
		kknet.WithLogger(kklog.GetConsoleLogger()),
		kknet.WithPingInterval(5*time.Second),
	)

	srv := kkws.NewServer(addr, echoHandler, opts)
	echoHandler.server = srv

	if err := srv.Start(); err != nil {
		panic(err)
	}
	defer srv.Stop()

	kklog.Infof("[examws] server listening on %s", addr)

	// 客户端连接并收发
	serverURL := "ws://" + addr + "/ws"
	clientHandler := &wsRecvHandler{ch: recvCh}

	client := kkws.NewClient(serverURL, clientHandler, kknet.ApplyOptions(
		kknet.WithRawHandler(clientHandler),
		kknet.WithLogger(kklog.GetConsoleLogger()),
		kknet.WithPingInterval(5*time.Second),
		kknet.WithIsNeedReconnect(false),
	))

	if err := client.Connect(); err != nil {
		panic(err)
	}
	defer client.Close()

	time.Sleep(100 * time.Millisecond)

	// 发送消息
	payload := []byte("hello examws")
	bb, err := streamTool.Pack(payload)
	if err != nil {
		panic(err)
	}
	if err := client.SendBuffer(bb); err != nil {
		panic(err)
	}

	// 等待回显
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

	fmt.Println("examws demo ok")
}

type wsEchoHandler struct {
	server kknet.IServer
}

func (h *wsEchoHandler) OnConnect(c kknet.IConn) {
	kklog.Infof("[examws] client connected: connID=%d", c.ID())
}

func (h *wsEchoHandler) OnClose(c kknet.IConn, err error) {
	kklog.Infof("[examws] client closed: connID=%d", c.ID())
}

func (h *wsEchoHandler) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	if data == nil || h.server == nil {
		if data != nil {
			kkbuffer.Put(data)
		}
		return
	}
	// 回显：收到的是 [length,message]，原样发回（无需再 Pack）
	bb := kkbuffer.GetWithCapacity(len(data.Bytes()))
	bb.B = append(bb.B[:0], data.Bytes()...)
	kkbuffer.Put(data)
	if err := h.server.SendBuffer(connID, bb); err != nil {
		kklog.Errorf("[examws] send error: %v", err)
		kkbuffer.Put(bb)
	}
}

type wsRecvHandler struct {
	ch chan []byte
}

func (h *wsRecvHandler) OnConnect(c kknet.IConn)          {}
func (h *wsRecvHandler) OnClose(c kknet.IConn, err error) {}

func (h *wsRecvHandler) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
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
