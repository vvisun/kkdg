package ccgame

import (
	"errors"

	"github.com/vvisun/kkdg/kkapp"
)

type Option struct {
	TransType    kkapp.TransType
	RpcAddr      string
	DiscoveryUrl string
	ClusterUrl   string
}

func DefaultOption() Option {
	return Option{
		TransType: kkapp.TransTypeNats,
	}
}

func validateOption(opt *Option) error {
	if opt.TransType == kkapp.TransTypeRpc || opt.TransType == kkapp.TransTypeShard {
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

func WithTransType(transType kkapp.TransType) func(o *Option) {
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
