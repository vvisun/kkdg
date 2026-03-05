package main

import (
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/kknet/msgreceiver"
	"github.com/vvisun/kkdg/other/examples/examapp/ptoexam"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

var tcpAddr = "127.0.0.1:19090"
var autoId int64 = 0

func initMsgs() {
	router := kkapp.GetMsgPacket().GetRouter()
	router.Register(1, &ptoexam.Msg1Req{}, "logic")
	router.Register(2, &ptoexam.Msg1Resp{}, "logic")
	router.Register(3, &ptoexam.Msg2Broadcast{}, "logic")
}

func main() {
	initMsgs()

	client := runOneClient()

	// 等待信号退出
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	<-signalCh
	kklog.Infof("receive interrupt signal, exit")
	client.Close()
	os.Exit(0)
}

func runOneClient() kknet.IClient {
	msgReceiver := msgreceiver.NewMsgReceiver[kknet.CONN_ID](kkapp.GetMsgPacket())
	gh := &gameHandler{}
	msgreceiver.RegisterMsgHandler(msgReceiver, gh.onMsg1Req)
	msgreceiver.RegisterMsgHandler(msgReceiver, gh.onMsg1Resp)
	msgreceiver.RegisterMsgHandler(msgReceiver, gh.onMsg2Broadcast)

	handler := &clientHandler{}

	opts := kknet.ApplyOptions(
		kknet.WithRawHandler(msgReceiver),
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

	return client
}

func sendMsg(client kknet.IClient) {
	curId := atomic.AddInt64(&autoId, 1)
	payload := []byte("hello")
	bb, err := kkpacket.EncodeStream(
		&ptoexam.Msg1Req{ID: int(curId), Data: string(payload)},
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
}

func (h *clientHandler) OnConnect(kknet.IConn) {

}

func (h *clientHandler) OnClose(kknet.IConn, error) {

}

type gameHandler struct {
}

func (h *gameHandler) onMsg1Req(sessionID kknet.CONN_ID, msg *ptoexam.Msg1Req) error {
	kklog.Infof("onMsg1Req: %v", msg)
	return nil
}

func (h *gameHandler) onMsg1Resp(sessionID kknet.CONN_ID, msg *ptoexam.Msg1Resp) error {
	kklog.Infof("onMsg1Resp: %v", msg)
	return nil
}

func (h *gameHandler) onMsg2Broadcast(sessionID kknet.CONN_ID, msg *ptoexam.Msg2Broadcast) error {
	kklog.Infof("onMsg2Broadcast: %v", msg)
	return nil
}
