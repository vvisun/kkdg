package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/component"
	"github.com/vvisun/kkdg/kkapp/comps/ccgame"
	"github.com/vvisun/kkdg/kkapp/transport/gametrans"
	"github.com/vvisun/kkdg/kknet/msgreceiver"
	"github.com/vvisun/kkdg/other/examples/examapp"
	"github.com/vvisun/kkdg/other/examples/examapp/ptoexam"
	"github.com/vvisun/kkdg/utils/kklog"
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
	gameNode := kkapp.NewNodeInfo(examapp.LogicNodeID, kkapp.NodeTypeLogic, "", "")
	gameApp := component.NewApplication(gameNode, nil, kkapp.ApplyOptions())
	ptoexam.InitMsgs(gameApp.GetOptions().ClientMsgPacket.GetRouter())
	game := ccgame.NewGameComponent(ccgame.Options{
		TransType:       examapp.UseTransType,
		TransServerAddr: examapp.RpcAddr,
		DiscoveryUrl:    examapp.NatsURL,
		ClusterUrl:      examapp.NatsURL,
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
	msgreceiver.RegisterMsgHandler(msgReceiver, gh.onMsg1Req)
	msgreceiver.RegisterMsgHandler(msgReceiver, gh.onLoginReq)

	return gameApp
}

type gameHandler struct {
	transportor gametrans.ITransportor
	nodeInfo    *kkapp.NodeInfo
}

func (h *gameHandler) onMsg1Req(sessionID string, msg *ptoexam.Msg1Req) error {
	if msg.ID%10000 == 0 {
		kklog.Debugf("onMsg1Req: sessionID=%s, msg=%v", sessionID, msg)
	}
	h.transportor.SendToClient(sessionID, &ptoexam.Msg1Resp{ID: msg.ID, Name: "hello"})
	return nil
}

func (h *gameHandler) onLoginReq(sessionID string, msg *ptoexam.LoginReq) error {
	kklog.Debugf("onLoginReq: sessionID=%s, msg=%v", sessionID, msg)

	h.transportor.NotifyClientLoginLogout(sessionID, msg.UserID, true)

	resp := &ptoexam.LoginResp{
		UserID:    msg.UserID,
		SessionID: sessionID,
		Token:     "token",
		UserData:  "userData",
	}
	h.transportor.SendToClient(sessionID, resp)

	return nil
}
