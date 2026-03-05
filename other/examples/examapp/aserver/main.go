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
	"github.com/vvisun/kkdg/utils/kklog"
)

var (
	natsURL  = "nats://127.0.0.1:4222"
	tcpAddr  = "127.0.0.1:19090"
	settings = map[string]string{"nats_url": natsURL}
)

func main() {
	initMsgs()

	// gate 节点
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

	// t.Cleanup(func() { _ = gateApp.Stop() })

	// game 节点（nodeType 必须为 logic 以匹配 gate 的 LogicNodeType）
	gameNode := kkapp.NewNodeInfo("game1", kkapp.NodeTypeLogic, "127.0.0.1:0", "", settings)
	gameApp := component.NewApplication(gameNode)
	game := ccgame.NewGameComponent()

	msgReceiver := game.GetMsgReceiver()
	gh := &gameHandler{}
	gh.sendToClient = game.SendToClient
	msgreceiver.RegisterMsgHandler(msgReceiver, gh.onMsgTest1)
	msgreceiver.RegisterMsgHandler(msgReceiver, gh.onMsgTest2)
	msgreceiver.RegisterMsgHandler(msgReceiver, gh.onMsgTest3)

	if err := gameApp.AddComponent(game); err != nil {
		kklog.Errorf("add game: %v", err)
	}
	if err := gameApp.Start(); err != nil {
		kklog.Errorf("game start: %v", err)
	}

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	<-signalCh
	kklog.Infof("receive interrupt signal, exit")
	gameApp.Stop()
	gateApp.Stop()
	os.Exit(0)
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
	sendToClient func(sessionID string, msg any) error
}

func (h *gameHandler) onMsgTest1(sessionID string, msg *MsgTest1) error {
	kklog.Infof("onMsgTest1: %v", msg)
	h.sendToClient(sessionID, msg)
	return nil
}

func (h *gameHandler) onMsgTest2(sessionID string, msg *MsgTest2) error {
	kklog.Infof("onMsgTest2: %v", msg)
	return nil
}

func (h *gameHandler) onMsgTest3(sessionID string, msg *MsgTest3) error {
	kklog.Infof("onMsgTest3: %v", msg)
	return nil
}
