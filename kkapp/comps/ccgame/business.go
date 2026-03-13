package ccgame

import (
	"errors"
	"sync"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/component"
	"github.com/vvisun/kkdg/kkapp/transport"
	"github.com/vvisun/kkdg/kkapp/transport/gametrans"
	"github.com/vvisun/kkdg/kkapp/transport/gametrans/gametransnats"
	"github.com/vvisun/kkdg/kkapp/transport/gametrans/gametransrpc"
	"github.com/vvisun/kkdg/kkapp/transport/gametrans/gametransshard"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/msgreceiver"
	"github.com/vvisun/kkdg/remotes/kkcluster"
	"github.com/vvisun/kkdg/remotes/kkcluster/cnats"
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
	"github.com/vvisun/kkdg/remotes/kkdiscovery/dnats"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/xreflect"
)

func NewGameComponent(opt Option) *gameComponent {
	if err := validateOption(&opt); err != nil {
		kklog.PanicErr(err)
	}
	streamTool := kkapp.GetStreamTool()
	messageTool := kkapp.GetMsgPacket()
	packetTool := kkpacket.NewFullPacket(streamTool, messageTool)
	return &gameComponent{
		msgReceiver:    msgreceiver.NewMsgReceiver[string](packetTool),
		sessionManager: gametrans.NewSessionManager(),
		opt:            opt,
	}
}

// 业务服：游戏服
type gameComponent struct {
	component.Component
	discovery          kkdiscovery.IDiscovery
	cluster            kkcluster.ICluster
	msgReceiver        *msgreceiver.MsgReceiver[string]
	sessionManager     *gametrans.SessionManager
	transportor        gametrans.ITransportor
	opt                Option
	transOkListeners   []func(transportor gametrans.ITransportor)
	transOkListenersMu sync.RWMutex
}

func (slf *gameComponent) GetCompName() string {
	return "game"
}

var _ kkapp.IComponent = (*gameComponent)(nil)

var _ actor.Actor = (*gameComponent)(nil)

func (slf *gameComponent) Receive(context actor.Context) {
	switch context.Message().(type) {
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
		discoveryOpts,
		kkdiscovery.ApplyOptions(),
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
		kkcluster.ApplyOptions(),
	)

	// 初始化 transportor
	switch slf.opt.TransType {
	case transport.TransTypeNats:
		transportor, err := gametransnats.NewTransportorNats(slf.cluster, slf.msgReceiver, slf.sessionManager, slf.GetApplication().GetNodeInfo())
		if err != nil {
			return err
		}
		slf.transportor = transportor
	case transport.TransTypeRpc:
		transportor, err := gametransrpc.NewTransportorRpc(slf.sessionManager, slf.msgReceiver, slf.GetApplication(), slf.opt.RpcAddr)
		if err != nil {
			return err
		}
		slf.transportor = transportor
	case transport.TransTypeShard:
		nodeId := slf.GetApplication().GetNodeId()
		nodeType := slf.GetApplication().GetNodeType()
		transportor, err := gametransshard.NewTransportorShard(slf.sessionManager, slf.msgReceiver, slf.opt.RpcAddr, nodeId, nodeType)
		if err != nil {
			return err
		}
		slf.transportor = transportor
	default:
		return errors.New("invalid trans type: " + slf.opt.TransType)
	}

	slf.notifyTransportorOK()

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
	if slf.transportor != nil {
		if err := slf.transportor.Stop(); err != nil {
			kklog.Errorf("[ccgame] stop transportor error: %v", err)
		}
	}
	return nil
}

func (slf *gameComponent) GetMsgReceiver() *msgreceiver.MsgReceiver[string] {
	return slf.msgReceiver
}

func (slf *gameComponent) GetSessionManager() *gametrans.SessionManager {
	return slf.sessionManager
}

func (slf *gameComponent) GetTransportor() gametrans.ITransportor {
	return slf.transportor
}

// OnTransportorOK 注册 transportor 就绪回调函数。
//
//	注意：
//	 1. transportor 就绪回调函数在 transportor 就绪后立即执行，不会等待 transportor 的 Start 方法执行完成。
//	 2. 触发 transportor 就绪回调函数后，回调函数会被自动移除。
func (slf *gameComponent) OnTransportorOK(fn func(transportor gametrans.ITransportor)) {
	if fn == nil {
		return
	}
	slf.transOkListenersMu.Lock()
	for _, listener := range slf.transOkListeners {
		if xreflect.IsSameFunc(listener, fn) {
			return
		}
	}
	slf.transOkListeners = append(slf.transOkListeners, fn)
	slf.transOkListenersMu.Unlock()
}

func (slf *gameComponent) notifyTransportorOK() {
	slf.transOkListenersMu.RLock()
	listeners := make([]func(transportor gametrans.ITransportor), len(slf.transOkListeners))
	copy(listeners, slf.transOkListeners)
	slf.transOkListenersMu.RUnlock()
	for _, listener := range listeners {
		listener(slf.transportor)
	}
	slf.transOkListeners = make([]func(transportor gametrans.ITransportor), 0)
}
