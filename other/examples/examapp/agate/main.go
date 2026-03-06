package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/component"
	"github.com/vvisun/kkdg/kkapp/comps/ccgate"
	"github.com/vvisun/kkdg/other/examples/examapp"
	"github.com/vvisun/kkdg/other/examples/examapp/ptoexam"
	"github.com/vvisun/kkdg/utils/kklog"
)

var (
	natsURL = examapp.NatsURL
	tcpAddr = examapp.GateTCPAddr
	wsAddr  = examapp.GateWSAddr
	rpcAddr = examapp.RpcAddr
)

func main() {
	ptoexam.InitMsgs(kkapp.GetMsgPacket().GetRouter())

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
	gateNode := kkapp.NewNodeInfo("gate1", kkapp.NodeTypeGate, tcpAddr, "", nil)
	gateApp := component.NewApplication(gateNode)
	gateOpt := ccgate.Option{
		TCPAddr:       tcpAddr,
		WSAddr:        wsAddr,
		RpcAddr:       rpcAddr,
		NatsURL:       natsURL,
		LogicNodeType: kkapp.NodeTypeLogic,
		TransType:     examapp.UseTransType,
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
