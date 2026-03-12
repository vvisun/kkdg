package ccgame

import (
	"errors"

	"github.com/vvisun/kkdg/kkapp/transport"
)

type Option struct {
	TransType    transport.TransType
	RpcAddr      string
	DiscoveryUrl string
	ClusterUrl   string
}

func DefaultOption() Option {
	return Option{
		TransType: transport.TransTypeNats,
	}
}

func validateOption(opt *Option) error {
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
	return nil
}

func ApplyOption(opt *Option, opts ...func(o *Option)) *Option {
	for _, o := range opts {
		o(opt)
	}
	return opt
}

func WithTransType(transType transport.TransType) func(o *Option) {
	return func(o *Option) {
		o.TransType = transType
	}
}

func WithRpcAddr(rpcAddr string) func(o *Option) {
	return func(o *Option) {
		o.RpcAddr = rpcAddr
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
