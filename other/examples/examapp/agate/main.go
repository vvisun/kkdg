package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/component"
	"github.com/vvisun/kkdg/kkapp/comps/ccgate"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/other/examples/examapp"
	"github.com/vvisun/kkdg/other/examples/examapp/ptoexam"
	"github.com/vvisun/kkdg/utils/kklog"
)

func main() {
	examapp.ParseFlags(nil)
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
	gateNode := kkapp.NewNodeInfo("gate1", kkapp.NodeTypeGate, examapp.GateTCPAddr, "", nil)
	gateApp := component.NewApplication(gateNode, nil)
	gateOpt := ccgate.Option{
		TCPAddr:       examapp.GateTCPAddr,
		WSAddr:        examapp.GateWSAddr,
		RpcAddr:       examapp.RpcAddr,
		DiscoveryUrl:  examapp.NatsURL,
		ClusterUrl:    examapp.NatsURL,
		LogicNodeType: kkapp.NodeTypeLogic,
		TransType:     examapp.UseTransType,
		RecvQueueFullCallback: func(conn kknet.IConn) {
			conn.SendMsg(&ptoexam.TipServerBusy{
				Code:    1,
				Message: "server busy",
			})
		},
	}
	gate := ccgate.NewGateComponent(gateOpt, kknet.DefaultOptions())
	if err := gateApp.AddComponent(gate); err != nil {
		kklog.Errorf("add gate: %v", err)
	}
	if err := gateApp.Start(); err != nil {
		kklog.Errorf("gate start: %v", err)
	}
	return gateApp
}
