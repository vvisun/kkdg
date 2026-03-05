package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/component"
	"github.com/vvisun/kkdg/kkapp/comps/ccgate"
	"github.com/vvisun/kkdg/other/examples/examapp/ptoexam"
	"github.com/vvisun/kkdg/utils/kklog"
)

var (
	natsURL  = "nats://127.0.0.1:4222"
	tcpAddr  = "127.0.0.1:19090"
	settings = map[string]string{"nats_url": natsURL}
)

func initMsgs() {
	router := kkapp.GetMsgPacket().GetRouter()
	router.Register(1, &ptoexam.Msg1Req{}, "test")
	router.Register(2, &ptoexam.Msg1Resp{}, "test")
	router.Register(3, &ptoexam.Msg2Broadcast{}, "test")
}

func main() {
	initMsgs()

	// gate 节点
	gateApp := runGate()

	// 等待信号退出
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	<-signalCh
	kklog.Infof("receive interrupt signal, exit")
	gateApp.Stop()
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
