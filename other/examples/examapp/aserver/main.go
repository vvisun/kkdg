package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/component"
	"github.com/vvisun/kkdg/kkapp/comps/ccgame"
	"github.com/vvisun/kkdg/kknet/msgreceiver"
	"github.com/vvisun/kkdg/other/examples/examapp"
	"github.com/vvisun/kkdg/other/examples/examapp/ptoexam"
	"github.com/vvisun/kkdg/utils/kklog"
)

func main() {
	examapp.ParseFlags(nil)
	ptoexam.InitMsgs(kkapp.GetMsgPacket().GetRouter())

	// game 节点
	gameApp := runGame()

	// 等待信号退出
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	<-signalCh
	kklog.Infof("receive interrupt signal, exit")
	gameApp.Stop()
	os.Exit(0)
}

func runGame() *component.Application {
	// game 节点，nodeType 必须为 logic 以匹配 gate 的 LogicNodeType
	gameNode := kkapp.NewNodeInfo(examapp.LogicNodeID, kkapp.NodeTypeLogic, "127.0.0.1:0", "", nil)
	gameApp := component.NewApplication(gameNode)
	game := ccgame.NewGameComponent(ccgame.Option{
		TransType:    examapp.UseTransType,
		RpcAddr:      examapp.RpcAddr,
		DiscoveryUrl: examapp.NatsURL,
		ClusterUrl:   examapp.NatsURL,
	})

	msgReceiver := game.GetMsgReceiver()
	gh := &gameHandler{}
	gh.sendToClient = game.SendToClient
	msgreceiver.RegisterMsgHandler(msgReceiver, gh.onMsg1Req)
	msgreceiver.RegisterMsgHandler(msgReceiver, gh.onMsg1Resp)
	msgreceiver.RegisterMsgHandler(msgReceiver, gh.onMsg2Broadcast)

	if err := gameApp.AddComponent(game); err != nil {
		kklog.Errorf("add game: %v", err)
	}
	if err := gameApp.Start(); err != nil {
		kklog.Errorf("game start: %v", err)
	}
	return gameApp
}

type gameHandler struct {
	sendToClient func(sessionID string, msg any) error
}

func (h *gameHandler) onMsg1Req(sessionID string, msg *ptoexam.Msg1Req) error {
	kklog.Infof("onMsg1Req: %v", msg)
	h.sendToClient(sessionID, &ptoexam.Msg1Resp{ID: msg.ID, Name: "hello"})
	return nil
}

func (h *gameHandler) onMsg1Resp(sessionID string, msg *ptoexam.Msg1Resp) error {
	kklog.Infof("onMsg1Resp: %v", msg)
	return nil
}

func (h *gameHandler) onMsg2Broadcast(sessionID string, msg *ptoexam.Msg2Broadcast) error {
	kklog.Infof("onMsg2Broadcast: %v", msg)
	return nil
}
