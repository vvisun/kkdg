package ccgame

import (
	"errors"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/component"
	"github.com/vvisun/kkdg/kkapp/comps/ccgame/gametrans"
	"github.com/vvisun/kkdg/kkapp/comps/ccgame/gametrans/gametransnats"
	"github.com/vvisun/kkdg/kkapp/comps/ccgame/gametrans/gametransrpc"
	"github.com/vvisun/kkdg/kkapp/comps/ccgame/gametrans/gametransshard"
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
	discovery      kkdiscovery.IDiscovery
	cluster        kkcluster.ICluster
	msgReceiver    *msgreceiver.MsgReceiver[string]
	sessionManager *gametrans.SessionManager
	transportor    gametrans.ITransportor
	opt            Option
}

func (slf *gameComponent) GetCompName() string {
	return "game"
}

var _ kkapp.IComponent = (*gameComponent)(nil)

var _ actor.Actor = (*gameComponent)(nil)

func (slf *gameComponent) Receive(context actor.Context) {
	switch context.Message().(type) {
	case *actor.Started:
		slf.OnStart()
	case *actor.Stopping:
		slf.OnStop()
	}
}

func (slf *gameComponent) OnInit() error {
	// 初始化 discovery
	discoveryOpts := dnats.ApplyNatsOptions(dnats.WithUrl(slf.opt.DiscoveryUrl))
	slf.discovery = dnats.NewNatsDiscovery(
		"logic."+slf.GetApplication().GetNodeId(),
		slf.GetApplication().GetNodeInfo(),
		nil,
		discoveryOpts,
	)
	slf.discovery.SetInfoGetter(func() (int, int) {
		return slf.sessionManager.OnlineCount(), kkdiscovery.NodeStatusOnline
	})

	// 初始化 cluster
	clusterOpts := cnats.ApplyNatsOptions(cnats.WithUrl(slf.opt.ClusterUrl))
	slf.cluster = cnats.NewNatsCluster(
		slf.GetApplication().GetNodeId(),
		slf.GetApplication().GetNodeType(),
		slf.discovery,
		clusterOpts,
	)

	// 初始化 transportor
	switch slf.opt.TransType {
	case kkapp.TransTypeNats:
		slf.transportor = gametransnats.NewTransportorNats(slf.cluster, slf.msgReceiver, slf.sessionManager)
	case kkapp.TransTypeRpc:
		slf.transportor = gametransrpc.NewTransportorRpc(slf.sessionManager, slf.msgReceiver, slf.GetApplication(), slf.opt.RpcAddr)
	case kkapp.TransTypeShard:
		nodeId := slf.GetApplication().GetNodeId()
		nodeType := slf.GetApplication().GetNodeType()
		slf.transportor = gametransshard.NewTransportorShard(slf.sessionManager, slf.msgReceiver, slf.opt.RpcAddr, nodeId, nodeType)
	default:
		return errors.New("invalid trans type: " + slf.opt.TransType)
	}

	return nil
}

func (slf *gameComponent) OnStart() error {
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

func (slf *gameComponent) OnStop() error {
	if slf.cluster != nil {
		slf.cluster.Stop()
	}
	if slf.discovery != nil {
		if err := slf.discovery.Stop(); err != nil {
			kklog.Errorf("[ccgame] stop discovery error: %v", err)
		}
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
