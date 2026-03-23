package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/component"
	"github.com/vvisun/kkdg/kkapp/kkactor"
	"github.com/vvisun/kkdg/kkapp/transport/gametrans"
	"github.com/vvisun/kkdg/remotes/kkcluster"
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/zothers/internal/demo1/comps"
	"github.com/vvisun/kkdg/zothers/internal/demo1/comps/ccgame"
	"github.com/vvisun/kkdg/zothers/internal/demo1/comps/msgreceiver"
	"github.com/vvisun/kkdg/zothers/internal/demo1/examapp"
	"github.com/vvisun/kkdg/zothers/internal/demo1/examapp/ptoexam"
)

func main() {
	examapp.ParseFlags(nil)

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
	af := kkactor.NewActorFramework()
	gameNode := kkapp.NewNodeInfo(examapp.LogicNodeID, comps.NodeTypeLogic, "", "")
	gameApp := component.NewApplication(gameNode, af, kkapp.ApplyOptions())
	ptoexam.InitMsgs(gameApp.GetOptions().ClientMsgPacket.GetRouter())
	game := ccgame.NewGameComponent(ccgame.Options{
		TransType:       examapp.UseTransType,
		TransServerAddr: examapp.TransAddr,
		DiscoveryOpts: kkdiscovery.ApplyOptions(
			kkdiscovery.WithUrl(examapp.NatsURL),
		),
		ClusterOpts: kkcluster.ApplyOptions(
			kkcluster.WithUrl(examapp.NatsURL),
		),
	})

	if err := gameApp.AddComponent(game); err != nil {
		kklog.Errorf("add game: %v", err)
	}
	if err := gameApp.Start(); err != nil {
		kklog.Errorf("game start: %v", err)
	}

	msgReceiver := game.GetMsgReceiver()
	gh := &gameHandler{
		transportor: game.GetTransportor(),
		nodeInfo:    gameApp.GetNodeInfo(),
	}
	msgreceiver.RegisterSessionMsgHandler(msgReceiver, gh.onMsg1Req)
	msgreceiver.RegisterSessionMsgHandler(msgReceiver, gh.onLoginReq)

	return gameApp
}

type gameHandler struct {
	transportor gametrans.ITransportor
	nodeInfo    *kkapp.NodeInfo
}

func (h *gameHandler) onMsg1Req(sessionID string, msg *ptoexam.Msg1Req) {
	if msg.ID%10000 == 0 {
		kklog.Debugf("onMsg1Req: sessionID=%s, msg=%v", sessionID, msg)
	}
	h.transportor.SendToClient(sessionID, &ptoexam.Msg1Resp{ID: msg.ID, Name: "hello"})
}

func (h *gameHandler) onLoginReq(sessionID string, msg *ptoexam.LoginReq) {
	kklog.Debugf("onLoginReq: sessionID=%s, msg=%v", sessionID, msg)

	h.transportor.NotifyClientLoginLogout(sessionID, msg.UserID, true)

	resp := &ptoexam.LoginResp{
		UserID:    msg.UserID,
		SessionID: sessionID,
		Token:     "token",
		UserData:  "userData",
	}
	h.transportor.SendToClient(sessionID, resp)
}
