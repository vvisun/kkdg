package main

import (
	"net/url"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/msgreceiver"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkgws"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/zothers/examples/examapp"
	"github.com/vvisun/kkdg/zothers/examples/examapp/ptoexam"
)

var autoId int64 = 0

type clientInfo struct {
	index    int
	userId   int64
	client   kknet.IClient
	hasLogin bool
}

var (
	sendedId   int64 = 0
	receivedId int64 = 0

	clientMap []clientInfo
)

func main() {
	examapp.ParseFlags(nil)

	clientMap = make([]clientInfo, examapp.ClientConnNum+16)

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
	appOpts := kkapp.ApplyOptions()
	streamTool := appOpts.StreamTool
	messageTool := appOpts.ClientMsgPacket
	packetTool := kkpacket.NewFullPacket(streamTool, messageTool)
	msgReceiver := msgreceiver.NewMsgReceiver[kknet.CONN_ID](packetTool)
	ptoexam.InitMsgs(appOpts.ClientMsgPacket.GetRouter())

	index := int(atomic.LoadInt64(&autoUserId))

	gh := &gameHandler{index: index}
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
	clientMap[index] = clientInfo{
		index:    index,
		userId:   userId,
		client:   client,
		hasLogin: true,
	}

	//定时发送消息
	go func() {
		for {
			ok := clientMap[index].hasLogin

			if ok {
				time.Sleep(examapp.ClientSendInterval)
			} else {
				time.Sleep(1500 * time.Millisecond)
			}

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
	index int
}

func (h *gameHandler) onLoginResp(connId kknet.CONN_ID, msg *ptoexam.LoginResp) {
	kklog.Infof("LoginResp: connId=%d, %v", connId, msg)
	if h.index < 0 || h.index >= len(clientMap) {
		return
	}
	clientMap[h.index].userId = msg.UserID
	clientMap[h.index].hasLogin = true
}

func (h *gameHandler) onMsg1Resp(connId kknet.CONN_ID, msg *ptoexam.Msg1Resp) {
	atomic.StoreInt64(&receivedId, int64(msg.ID))
	if receivedId%5000 != 0 {
		return
	}
	kklog.Infof("Msg1Resp: connId=%d, sendedId=%d, receivedId=%d diff=%d",
		connId,
		atomic.LoadInt64(&sendedId),
		atomic.LoadInt64(&receivedId),
		atomic.LoadInt64(&sendedId)-atomic.LoadInt64(&receivedId))
}

func (h *gameHandler) onMsg2Broadcast(connId kknet.CONN_ID, msg *ptoexam.Msg2Broadcast) {
	kklog.Infof("Msg2Broadcast: connId=%d, %v", connId, msg)
}

func (h *gameHandler) onTipServerBusy(connId kknet.CONN_ID, msg *ptoexam.TipServerBusy) {
	atomic.AddInt64(&receivedId, 1)
	kklog.Infof("TipServerBusy: connId=%d, sendedId=%d, receivedId=%d diff=%d",
		connId,
		atomic.LoadInt64(&sendedId),
		atomic.LoadInt64(&receivedId),
		atomic.LoadInt64(&sendedId)-atomic.LoadInt64(&receivedId))
}
