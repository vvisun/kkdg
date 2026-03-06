package ccgame

import (
	"errors"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/component"
	"github.com/vvisun/kkdg/kkapp/comps/ccgame/gametrans"
	"github.com/vvisun/kkdg/kkapp/comps/ccgame/gametrans/gametransnats"
	"github.com/vvisun/kkdg/kkapp/comps/ccgame/gametrans/gametransrpc"
	"github.com/vvisun/kkdg/kknet/msgreceiver"
	"github.com/vvisun/kkdg/remotes/kkcluster"
	"github.com/vvisun/kkdg/remotes/kkcluster/cnats"
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
	"github.com/vvisun/kkdg/remotes/kkdiscovery/dnats"
	"github.com/vvisun/kkdg/utils/kklog"
)

func NewGameComponent(opt Option) *gameComponent {
	if err := validateOption(&opt); err != nil {
		panic(err)
	}
	return &gameComponent{
		msgReceiver:    msgreceiver.NewMsgReceiver[string](kkapp.GetMsgPacket()),
		sessionManager: gametrans.NewSessionManager(),
		opt:            opt,
	}
}

// 业务服：游戏服
type gameComponent struct {
	component.Component
	pid            *actor.PID
	discovery      kkdiscovery.IDiscovery
	cluster        kkcluster.ICluster
	msgReceiver    *msgreceiver.MsgReceiver[string]
	sessionManager *gametrans.SessionManager
	transportor    gametrans.ITransportor
	opt            Option
}

func (slf *gameComponent) GetID() string {
	return "game"
}

var _ component.IComponent = (*gameComponent)(nil)

func (slf *gameComponent) Init() error {
	nodeInfo := slf.GetApplication().GetNodeInfo()

	// discovery + cluster for receiving forwarded messages from gate
	opts := dnats.ApplyNatsOptions(dnats.WithUrl(slf.opt.NatsURL))
	slf.discovery = dnats.NewNatsDiscovery("logic."+slf.GetApplication().GetNodeId(), nodeInfo, nil, opts)
	slf.cluster = cnats.NewNatsCluster(
		slf.GetApplication().GetNodeId(),
		slf.GetApplication().GetNodeType(),
		slf.discovery,
		opts,
	)

	slf.discovery.SetInfoGetter(func() (int, int) {
		return slf.sessionManager.OnlineCount(), kkdiscovery.NodeStatusOnline
	})

	switch slf.opt.TransType {
	case kkapp.TransTypeNats:
		slf.transportor = gametransnats.NewTransportorNats(slf.cluster, slf.msgReceiver, slf.sessionManager)
	case kkapp.TransTypeRpc:
		slf.transportor = gametransrpc.NewTransportorRpc(slf.sessionManager, slf.msgReceiver, slf.GetApplication(), slf.opt.RpcAddr)
	default:
		return errors.New("invalid trans type: " + slf.opt.TransType)
	}

	return nil
}

func (slf *gameComponent) Start() error {
	if slf.discovery != nil {
		if err := slf.discovery.Start(); err != nil {
			return err
		}
	}
	if slf.cluster != nil {
		if err := slf.cluster.Start(); err != nil {
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

func (slf *gameComponent) GetSessionManager() *gametrans.SessionManager {
	return slf.sessionManager
}
