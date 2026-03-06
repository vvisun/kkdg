package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/component"
	"github.com/vvisun/kkdg/kkapp/comps/ccgame"
	"github.com/vvisun/kkdg/kkapp/comps/ccgate"
	"github.com/vvisun/kkdg/kknet/msgreceiver"
	"github.com/vvisun/kkdg/other/examples/examapp/ptoexam"
	"github.com/vvisun/kkdg/utils/kklog"
)

var (
	natsURL  = "nats://127.0.0.1:4222"
	tcpAddr  = "127.0.0.1:19090"
	settings = map[string]string{"nats_url": natsURL}
	withGate = false
)

func main() {
	ptoexam.InitMsgs(kkapp.GetMsgPacket().GetRouter())

	// gate 节点
	var gateApp *component.Application
	if withGate {
		gateApp = runGate()
	}

	// game 节点
	gameApp := runGame()

	// 等待信号退出
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	<-signalCh
	kklog.Infof("receive interrupt signal, exit")
	gameApp.Stop()
	if gateApp != nil {
		gateApp.Stop()
	}
	os.Exit(0)
}

func runGate() *component.Application {
	gateNode := kkapp.NewNodeInfo("gate1", kkapp.NodeTypeGate, tcpAddr, "", settings)
	gateApp := component.NewApplication(gateNode)
	gateOpt := ccgate.Option{
		TCPAddr:       tcpAddr,
		NatsURL:       natsURL,
		LogicNodeType: kkapp.NodeTypeLogic,
	}
	gate := ccgate.NewGateComponent(gateOpt)
	if err := gateApp.AddComponent(gate); err != nil {
		kklog.Errorf("add gate: %v", err)
	}
	if err := gateApp.Start(); err != nil {
		kklog.Errorf("gate start: %v", err)
	}
	return gateApp
}

func runGame() *component.Application {
	// game 节点，nodeType 必须为 logic 以匹配 gate 的 LogicNodeType
	gameNode := kkapp.NewNodeInfo("game1", kkapp.NodeTypeLogic, "127.0.0.1:0", "", settings)
	gameApp := component.NewApplication(gameNode)
	game := ccgame.NewGameComponent(ccgame.Option{
		TransType: kkapp.TransTypeNats,
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
