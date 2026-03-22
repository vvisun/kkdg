package ccgame

import (
	"errors"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/component"
	"github.com/vvisun/kkdg/kkapp/msgreceiver"
	"github.com/vvisun/kkdg/kkapp/transport"
	"github.com/vvisun/kkdg/kkapp/transport/gametrans"
	"github.com/vvisun/kkdg/kkapp/transport/gametrans/gametransnats"
	"github.com/vvisun/kkdg/kkapp/transport/gametrans/gametransrpc"
	"github.com/vvisun/kkdg/kkapp/transport/gametrans/gametransshard"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/remotes/kkcluster"
	"github.com/vvisun/kkdg/remotes/kkcluster/cnats"
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
	"github.com/vvisun/kkdg/remotes/kkdiscovery/dnats"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/kkoption"
	"github.com/vvisun/kkdg/utils/xos"
)

func NewGameComponent(opt Options) *gameComponent {
	if err := validateOption(&opt); err != nil {
		kklog.PanicErr(err)
	}
	return &gameComponent{
		sessionManager: gametrans.NewSessionManager(xos.NumCPU() * 2),
		opt:            opt,
	}
}

// 业务服：游戏服
type gameComponent struct {
	component.Component
	discovery      kkdiscovery.IDiscovery
	cluster        kkcluster.ICluster
	msgReceiver    gametrans.ISessionMsgReceiver
	sessionManager *gametrans.SessionManager
	transportor    gametrans.ITransportor
	opt            Options
	discoverySubID uint64
}

func (slf *gameComponent) GetCompName() string {
	return "comp_game"
}

var _ kkapp.IComponent = (*gameComponent)(nil)

var _ actor.Actor = (*gameComponent)(nil)

func (slf *gameComponent) Receive(ctx actor.Context) {
	switch ctx.Message().(type) {
	case *actor.Stopping:
		slf.OnStop()
	}
}

func (slf *gameComponent) OnInit() error {
	// 初始化 discovery
	if slf.opt.DiscoveryOpts.Url != "" {
		slf.discovery = dnats.NewNatsDiscovery(
			slf.GetApplication().GetNodeInfo(),
			slf.opt.DiscoveryOpts,
		)
		slf.discoverySubID = kkdiscovery.GlobalEventMgr.Subscribe(kkdiscovery.EventDiscoveryStats, func(e *kkdiscovery.DiscoveryStatsEvent) {
			e.OnlineCount = slf.sessionManager.OnlineCount()
			e.Status = kkdiscovery.NodeStatusOnline
		})
	}

	// 初始化 cluster
	if slf.opt.ClusterOpts.Url != "" {
		clusterOpts := slf.opt.ClusterOpts
		if slf.discovery != nil {
			kkoption.ApplyOptionsTo(&clusterOpts,
				kkcluster.WithDiscovery(slf.discovery),
			)
		}
		slf.cluster = cnats.NewNatsCluster(
			slf.GetApplication().GetNodeId(),
			slf.GetApplication().GetNodeType(),
			clusterOpts,
		)
	}

	appOpts := slf.GetApplication().GetOptions()
	packetTool := kkpacket.NewFullPacket(appOpts.StreamTool, appOpts.ClientMsgPacket)
	msgReceiver := msgreceiver.NewSessionMsgReceiver[string](packetTool, slf.sessionManager.GetWorkersCount())
	slf.msgReceiver = msgReceiver

	transMsgPacket := kkpacket.NewMessagePacket(
		kkpacket.NewPacketHead(&kkpacket.PartUint32{}),
		appOpts.TransportorCodec,
		kkpacket.NewMsgRouter(),
	)

	// 初始化 transportor
	switch slf.opt.TransType {
	case transport.TransTypeNats:
		transportor, err := gametransnats.NewTransportorNats(
			slf.cluster,
			slf.msgReceiver,
			slf.sessionManager,
			slf.GetApplication().GetNodeInfo(),
			transMsgPacket,
			appOpts.ClientMsgPacket,
			appOpts.StreamTool,
			appOpts.StreamTool,
		)
		if err != nil {
			return err
		}
		slf.transportor = transportor
	case transport.TransTypeShard:
		transportor, err := gametransshard.NewTransportorShard(
			slf.sessionManager,
			slf.msgReceiver,
			slf.opt.TransServerAddr,
			slf.GetApplication().GetNodeInfo(),
			transMsgPacket,
			appOpts.ClientMsgPacket,
			appOpts.StreamTool,
			appOpts.StreamTool,
		)
		if err != nil {
			return err
		}
		slf.transportor = transportor
	case transport.TransTypeRpc:
		transportor, err := gametransrpc.NewTransportorRpc(
			slf.sessionManager,
			slf.msgReceiver,
			slf.GetApplication().GetNodeInfo(),
			slf.opt.TransServerAddr,
			appOpts.ClientMsgPacket,
			appOpts.StreamTool,
		)
		if err != nil {
			return err
		}
		slf.transportor = transportor
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
	kkdiscovery.GlobalEventMgr.UnsubscribeAll(kkdiscovery.EventDiscoveryStats)
	if slf.cluster != nil {
		slf.cluster.Stop()
	}
	if slf.discovery != nil {
		if err := slf.discovery.Stop(); err != nil {
			kklog.Errorf("[ccgame] stop discovery error: %v", err)
		}
	}
	if slf.transportor != nil {
		if err := slf.transportor.Stop(); err != nil {
			kklog.Errorf("[ccgame] stop transportor error: %v", err)
		}
	}
	return nil
}

func (slf *gameComponent) GetMsgReceiver() *msgreceiver.MsgReceiver[string] {
	return slf.msgReceiver.(*msgreceiver.MsgReceiver[string])
}

func (slf *gameComponent) GetSessionManager() *gametrans.SessionManager {
	return slf.sessionManager
}

func (slf *gameComponent) GetTransportor() gametrans.ITransportor {
	return slf.transportor
}
