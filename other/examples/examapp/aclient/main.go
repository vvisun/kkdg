package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/kknet/msgreceiver"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

var tcpAddr = "127.0.0.1:19090"

func main() {
	initMsgs()

	msgReceiver := msgreceiver.NewMsgReceiver[kknet.CONN_ID](kkapp.GetMsgPacket())
	gh := &gameHandler{}
	msgreceiver.RegisterMsgHandler(msgReceiver, gh.onMsgTest1)
	msgreceiver.RegisterMsgHandler(msgReceiver, gh.onMsgTest2)
	msgreceiver.RegisterMsgHandler(msgReceiver, gh.onMsgTest3)

	handler := &clientHandler{
		onRaw: func(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
			msgReceiver.OnRaw(connID, data)
		},
	}

	opts := kknet.ApplyOptions(
		kknet.WithRawHandler(handler),
	)
	client := kktcp.NewClient(tcpAddr, handler, opts)
	if err := client.Connect(); err != nil {
		kklog.Errorf("client connect: %v", err)
	}

	//定时发送消息
	go func() {
		for {
			time.Sleep(1 * time.Second)
			sendMsg(client)
		}
	}()

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	<-signalCh
	kklog.Infof("receive interrupt signal, exit")
	client.Close()
	os.Exit(0)
}

var autoId = 0

func sendMsg(client kknet.IClient) {
	autoId++
	payload := []byte("hello")
	bb, err := kkpacket.EncodeStream(
		&MsgTest1{ID: autoId, Data: string(payload)},
		kkpacket.DefaultStreamPacket(),
		kkapp.GetMsgPacket(),
	)
	if err != nil {
		kkbuffer.Put(bb)
		kklog.Errorf("pack: %v", err)
	} else {
		if err := client.SendBuffer(bb); err != nil {
			kklog.Errorf("send: %v", err)
		}
	}
}

type clientHandler struct {
	onRaw func(kknet.CONN_ID, *kkbuffer.ByteBuffer)
}

func (h *clientHandler) OnConnect(kknet.IConn)      {}
func (h *clientHandler) OnClose(kknet.IConn, error) {}

func (h *clientHandler) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	if h.onRaw != nil {
		h.onRaw(connID, data)
	} else {
		kkbuffer.Put(data)
	}
}

type (
	MsgTest1 struct {
		ID   int
		Data string
	}
	MsgTest2 struct {
		ID   int
		Data string
	}
	MsgTest3 struct {
		ID   int
		Data string
	}
)

func initMsgs() {
	router := kkapp.GetMsgPacket().GetRouter()
	router.Register(1, &MsgTest1{}, "test")
	router.Register(2, &MsgTest2{}, "test")
	router.Register(3, &MsgTest3{}, "test")
}

type gameHandler struct {
}

func (h *gameHandler) onMsgTest1(sessionID kknet.CONN_ID, msg *MsgTest1) error {
	kklog.Infof("onMsgTest1: %v", msg)
	return nil
}

func (h *gameHandler) onMsgTest2(sessionID kknet.CONN_ID, msg *MsgTest2) error {
	kklog.Infof("onMsgTest2: %v", msg)
	return nil
}

func (h *gameHandler) onMsgTest3(sessionID kknet.CONN_ID, msg *MsgTest3) error {
	kklog.Infof("onMsgTest3: %v", msg)
	return nil
}
