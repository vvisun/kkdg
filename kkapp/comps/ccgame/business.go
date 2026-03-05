package ccgame

import (
	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/component"
	"github.com/vvisun/kkdg/kknet/msgreceiver"
	"github.com/vvisun/kkdg/remotes/kkcluster"
	"github.com/vvisun/kkdg/remotes/kkcluster/cnats"
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
	"github.com/vvisun/kkdg/remotes/kkdiscovery/dnats"
	"github.com/vvisun/kkdg/utils/kklog"
)

func NewGameComponent() *gameComponent {
	return &gameComponent{
		msgReceiver:    msgreceiver.NewMsgReceiver[string](kkapp.GetMsgPacket()),
		sessionManager: newSessionManager(),
	}
}

// 业务服：游戏服
type gameComponent struct {
	component.Component
	pid            *actor.PID
	discovery      kkdiscovery.IDiscovery
	cluster        kkcluster.ICluster
	msgReceiver    *msgreceiver.MsgReceiver[string]
	sessionManager *SessionManager
	transportor    ITransportor
}

func (slf *gameComponent) GetID() string {
	return "game"
}

var _ component.IComponent = (*gameComponent)(nil)

func (slf *gameComponent) Init() error {
	nodeInfo := slf.GetApplication().GetNodeInfo()
	natsURL := ""
	if v, ok := nodeInfo.GetSetting("nats_url"); ok {
		natsURL = v
	}

	// discovery + cluster for receiving forwarded messages from gate
	opts := dnats.ApplyNatsOptions(dnats.WithUrl(natsURL))
	slf.discovery = dnats.NewNatsDiscovery("logic."+slf.GetApplication().GetNodeId(), nodeInfo, nil, opts)
	slf.cluster = cnats.NewNatsCluster(
		slf.GetApplication().GetNodeId(),
		slf.GetApplication().GetNodeType(),
		slf.discovery,
		opts,
	)

	slf.transportor = newTransportorNats(slf.cluster, slf.msgReceiver, slf.sessionManager)
	return nil
}

func (slf *gameComponent) Start() error {
	if slf.discovery != nil {
		if err := slf.discovery.Start(); err != nil {
			return err
		}
	}
	if slf.cluster != nil {
		if err := slf.cluster.Init(); err != nil {
			return err
		}
	}
	return nil
}

func (slf *gameComponent) Stop() error {
	if slf.cluster != nil {
		slf.cluster.Stop()
	}
	if slf.discovery != nil {
		if err := slf.discovery.Stop(); err != nil {
			kklog.Errorf("[ccgame] stop discovery error: %v", err)
		}
	}
	if slf.pid != nil {
		slf.GetApplication().GetActorSystem().Root.Stop(slf.pid)
		slf.pid = nil
	}
	return nil
}

func (slf *gameComponent) GetMsgReceiver() *msgreceiver.MsgReceiver[string] {
	return slf.msgReceiver
}

func (slf *gameComponent) SendToClient(sessionID string, msg any) error {
	return slf.transportor.SendToClient(sessionID, msg)
}

func (slf *gameComponent) GetSessionManager() *SessionManager {
	return slf.sessionManager
}
