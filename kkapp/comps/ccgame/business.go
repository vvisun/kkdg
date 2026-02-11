package ccgame

import (
	"github.com/asynkron/protoactor-go/actor"
	"github.com/nats-io/nats.go"
	"github.com/vvisun/kkdg/kkapp/component"
	"github.com/vvisun/kkdg/kknet/kkcluster"
	"github.com/vvisun/kkdg/kknet/kkcluster/cnats"
	"github.com/vvisun/kkdg/kknet/kkdiscovery"
	"github.com/vvisun/kkdg/kknet/kkdiscovery/dnats"
	"github.com/vvisun/kkdg/utils/kklog"
)

func NewGameComponent() *gameComponent {
	return &gameComponent{}
}

// 业务服：游戏服
type gameComponent struct {
	component.Component
	pid       *actor.PID
	discovery kkdiscovery.IDiscovery
	cluster   kkcluster.ICluster
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

	var opts []nats.Option
	if natsURL != "" {
		opts = append(opts, dnats.WithUrl(natsURL))
	}

	// discovery + cluster for receiving forwarded messages from gate
	slf.discovery = dnats.NewNatsDiscovery("logic."+slf.GetApplication().GetNodeId(), nodeInfo, nil, opts...)
	slf.cluster = cnats.NewNatsCluster(
		slf.GetApplication().GetNodeId(),
		slf.GetApplication().GetNodeType(),
		slf.discovery,
		opts...,
	)
	slf.cluster.SetPublishHandler(slf.onClusterPublish)
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

// onClusterPublish receives messages from gate, and (currently) echoes them back to gate.
// This makes the end-to-end forwarding path testable without business logic.
func (slf *gameComponent) onClusterPublish(sourceNodeID string, packet *kkcluster.ClusterPacket) {
	if slf.cluster == nil || packet == nil {
		return
	}
	if sourceNodeID == "" {
		return
	}

	resp := &kkcluster.ClusterPacket{
		FuncName: packet.FuncName,
		ArgBytes: append([]byte(nil), packet.ArgBytes...),
		Sid:      packet.Sid,
	}
	if err := slf.cluster.PublishRemote(sourceNodeID, resp); err != nil {
		kklog.Errorf("[ccgame] publish response to %s error: %v", sourceNodeID, err)
	}
}
