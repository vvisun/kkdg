package main

import (
	"net/url"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkgws"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/kknet/msgreceiver"
	"github.com/vvisun/kkdg/other/examples/examapp"
	"github.com/vvisun/kkdg/other/examples/examapp/ptoexam"
	"github.com/vvisun/kkdg/utils/kklog"
)

var autoId int64 = 0

var (
	sendedId   int64 = 0
	receivedId int64 = 0

	clientMap = make(map[kknet.IClient]map[string]any)
)

func main() {
	examapp.ParseFlags(nil)
	ptoexam.InitMsgs(kkapp.GetClientMsgPacket().GetRouter())

	clientMap = make(map[kknet.IClient]map[string]any)

	client := runOneClient()

	for i := 0; i < examapp.ClientConnNum; i++ {
		go func() {
			runOneClient()
		}()
		time.Sleep(examapp.ClientConnDelay)
	}

	// 等待信号退出
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	<-signalCh
	kklog.Infof("receive interrupt signal, exit")
	if client != nil {
		client.Close()
	}
	os.Exit(0)
}

var autoUserId int64 = 0

func runOneClient() kknet.IClient {
	streamTool := kkapp.GetStreamTool()
	messageTool := kkapp.GetClientMsgPacket()
	packetTool := kkpacket.NewFullPacket(streamTool, messageTool)
	msgReceiver := msgreceiver.NewMsgReceiver[kknet.CONN_ID](packetTool)
	gh := &gameHandler{}
	msgreceiver.RegisterMsgHandler(msgReceiver, gh.onLoginResp)
	msgreceiver.RegisterMsgHandler(msgReceiver, gh.onMsg1Resp)
	msgreceiver.RegisterMsgHandler(msgReceiver, gh.onMsg2Broadcast)
	msgreceiver.RegisterMsgHandler(msgReceiver, gh.onTipServerBusy)

	handler := &clientHandler{}

	var client kknet.IClient
	opts := kknet.ApplyOptions(
		kknet.WithStreamTool(streamTool),
		kknet.WithMsgPacket(messageTool),
		kknet.WithRawHandler(msgReceiver),
	)
	if examapp.GateWSAddr != "" {
		u := url.URL{Scheme: "ws", Host: examapp.GateWSAddr, Path: "/ws"}
		client = kkgws.NewClient(u.String(), handler, opts)
	} else if examapp.GateTCPAddr != "" {
		client = kktcp.NewClient(examapp.GateTCPAddr, handler, opts)
	} else {
		kklog.Errorf("tcpAddr or wsAddr is empty")
		return nil
	}
	if client == nil {
		kklog.Errorf("client is nil")
		return nil
	}
	if err := client.Connect(); err != nil {
		kklog.Errorf("client connect: %v", err)
		return nil
	}

	userId := atomic.AddInt64(&autoUserId, 1)
	gh.client = client
	clientMap[client] = make(map[string]any)
	clientMap[client]["user_id"] = userId

	//定时发送消息
	go func() {
		for {
			time.Sleep(examapp.ClientSendInterval)

			_, ok := clientMap[client]["login_session_id"]

			if ok {
				curId := atomic.AddInt64(&autoId, 1)
				atomic.StoreInt64(&sendedId, curId)
				payload := []byte("hello")
				msg := &ptoexam.Msg1Req{ID: int32(curId), Data: string(payload)}
				sendMsg(client, msg)
			} else {
				msg1 := &ptoexam.LoginReq{
					UserID:   userId,
					Token:    "token",
					Password: "password",
				}
				sendMsg(client, msg1)
			}
		}
	}()

	return client
}

func sendMsg(client kknet.IClient, msg any) {
	if err := client.SendMsg(msg); err != nil {
		kklog.Errorf("send: %v", err)
	}
}

type clientHandler struct {
}

func (h *clientHandler) OnConnect(kknet.IConn) {

}

func (h *clientHandler) OnClose(kknet.IConn, error) {

}

type gameHandler struct {
	client kknet.IClient
}

func (h *gameHandler) onLoginResp(sessionID kknet.CONN_ID, msg *ptoexam.LoginResp) error {
	kklog.Infof("onLoginResp: %v", msg)
	clientMap[h.client]["login_session_id"] = msg.SessionID
	return nil
}

func (h *gameHandler) onMsg1Resp(sessionID kknet.CONN_ID, msg *ptoexam.Msg1Resp) error {
	atomic.StoreInt64(&receivedId, int64(msg.ID))
	if receivedId%1000 != 0 {
		return nil
	}
	kklog.Infof("onMsg1Resp: sendedId=%d, receivedId=%d diff=%d",
		atomic.LoadInt64(&sendedId),
		atomic.LoadInt64(&receivedId),
		atomic.LoadInt64(&sendedId)-atomic.LoadInt64(&receivedId))
	return nil
}

func (h *gameHandler) onMsg2Broadcast(sessionID kknet.CONN_ID, msg *ptoexam.Msg2Broadcast) error {
	kklog.Infof("onMsg2Broadcast: %v", msg)
	return nil
}

func (h *gameHandler) onTipServerBusy(sessionID kknet.CONN_ID, msg *ptoexam.TipServerBusy) error {
	atomic.AddInt64(&receivedId, 1)
	kklog.Infof("onTipServerBusy: sendedId=%d, receivedId=%d diff=%d",
		atomic.LoadInt64(&sendedId),
		atomic.LoadInt64(&receivedId),
		atomic.LoadInt64(&sendedId)-atomic.LoadInt64(&receivedId))
	return nil
}
