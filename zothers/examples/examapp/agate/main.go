package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/component"
	"github.com/vvisun/kkdg/kkapp/comps"
	"github.com/vvisun/kkdg/kkapp/comps/ccgate"
	"github.com/vvisun/kkdg/kkapp/kkactor"
	"github.com/vvisun/kkdg/kkmetrics"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/remotes/kkcluster"
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/zothers/examples/examapp"
	"github.com/vvisun/kkdg/zothers/examples/examapp/ptoexam"
)

func main() {
	examapp.ParseFlags(nil)

	// gate 节点
	gateApp := runGate()

	// init metrics (Prometheus + OTel), expose /metrics on :2112
	ctx := context.Background()
	kkmetrics.AutoInit(ctx, ":2112")

	// 等待信号退出
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	<-signalCh
	kklog.Infof("receive interrupt signal, exit")
	gateApp.Stop()
	os.Exit(0)
}

func runGate() *component.Application {
	af := kkactor.NewActorFramework()
	gateNode := kkapp.NewNodeInfo("gate1", comps.NodeTypeGate, "", "")
	gateApp := component.NewApplication(gateNode, af, kkapp.ApplyOptions())
	ptoexam.InitMsgs(gateApp.GetOptions().ClientMsgPacket.GetRouter())
	gateOpt := ccgate.Options{
		TCPAddr:         examapp.GateTCPAddr,
		WSAddr:          examapp.GateWSAddr,
		TransServerAddr: examapp.TransAddr,
		DiscoveryOpts: kkdiscovery.ApplyOptions(
			kkdiscovery.WithUrl(examapp.NatsURL),
		),
		ClusterOpts: kkcluster.ApplyOptions(
			kkcluster.WithUrl(examapp.NatsURL),
		),
		TransType: examapp.UseTransType,
	}
	gate := ccgate.NewGateComponent(gateOpt, kknet.DefaultOptions())
	if err := gateApp.AddComponent(gate); err != nil {
		kklog.Errorf("add gate: %v", err)
	}
	if err := gateApp.Start(); err != nil {
		kklog.Errorf("gate start: %v", err)
	}
	gate.SetErrCallback(func(conn kknet.IConn, errCode ccgate.GateErrorCode) {
		switch errCode {
		case ccgate.ERR_RECV_QUEUE_FULL, ccgate.ERR_ALLOC_LOGIC_NODE_FAILED:
			conn.SendMsg(&ptoexam.TipServerBusy{
				Code:    1,
				Message: "服务器繁忙，请稍后再试",
			})
		case ccgate.ERR_CLIENT_INVALID_PACKET:
			conn.SendMsg(&ptoexam.ErrorMsg{
				Code:    1,
				Message: "协议版本不匹配，请更新客户端",
			})
			conn.Close()
		case ccgate.ERR_USER_KICKED:
			conn.SendMsg(&ptoexam.TipServerBusy{
				Code:    1,
				Message: "账号已在其他地方登录，请确认是否是本人操作",
			})
			conn.Close()
		}
	})
	return gateApp
}
