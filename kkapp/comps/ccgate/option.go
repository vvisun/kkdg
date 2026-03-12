package ccgate

import (
	"errors"

	"github.com/vvisun/kkdg/kkapp/transport"
	"github.com/vvisun/kkdg/kknet"
)

// Option configures the gate component.
type Option struct {
	TCPAddr string
	WSAddr  string
	RpcAddr string

	// DiscoveryUrl is the discovery server url used by discovery.
	DiscoveryUrl string
	// ClusterUrl is the cluster server url used by cluster.
	ClusterUrl string

	// LogicNodeType is the target node type for game logic nodes.
	// If empty, defaults to "logic".
	LogicNodeType string

	// 网关与逻辑服之间的转发通道类型，默认使用NATS。
	TransType transport.TransType

	// RecvQueueFullCallback is the callback function when the recv queue is full.
	// 可以考虑限流/提示服务器繁忙等。
	RecvQueueFullCallback func(conn kknet.IConn)
}

func DefaultOption() Option {
	return Option{
		TransType: transport.TransTypeNats,
	}
}

func ApplyOption(opt *Option, opts ...func(o *Option)) *Option {
	for _, o := range opts {
		o(opt)
	}
	return opt
}

func validateOption(opt *Option) error {
	if opt.TCPAddr == "" && opt.WSAddr == "" {
		return errors.New("tcp addr or ws addr is required")
	}
	if opt.TransType == transport.TransTypeRpc || opt.TransType == transport.TransTypeShard {
		if opt.RpcAddr == "" {
			return errors.New("rpc addr is required")
		}
	}
	if opt.DiscoveryUrl == "" {
		return errors.New("discovery url is required")
	}
	if opt.ClusterUrl == "" {
		return errors.New("cluster url is required")
	}
	if opt.TCPAddr == opt.WSAddr || opt.TCPAddr == opt.RpcAddr || opt.WSAddr == opt.RpcAddr {
		return errors.New("tcp addr, ws addr and rpc addr cannot be the same")
	}
	return nil
}

func WithTransType(transType transport.TransType) func(o *Option) {
	return func(o *Option) {
		o.TransType = transType
	}
}

func WithTCPAddr(tcpAddr string) func(o *Option) {
	return func(o *Option) {
		o.TCPAddr = tcpAddr
	}
}

func WithWSAddr(wsAddr string) func(o *Option) {
	return func(o *Option) {
		o.WSAddr = wsAddr
	}
}

func WithDiscoveryURL(natsURL string) func(o *Option) {
	return func(o *Option) {
		o.DiscoveryUrl = natsURL
	}
}

func WithClusterURL(clusterURL string) func(o *Option) {
	return func(o *Option) {
		o.ClusterUrl = clusterURL
	}
}

func WithLogicNodeType(logicNodeType string) func(o *Option) {
	return func(o *Option) {
		o.LogicNodeType = logicNodeType
	}
}

func WithRpcAddr(rpcAddr string) func(o *Option) {
	return func(o *Option) {
		o.RpcAddr = rpcAddr
	}
}
