package ccgame

import (
	"errors"

	"github.com/vvisun/kkdg/kkapp/transport"
	"github.com/vvisun/kkdg/remotes/kkcluster"
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
)

type Options struct {
	// 网关与业务服之间的转发通道类型。
	TransType transport.TransType
	// 转发层服务器地址。TransType为TransTypeRpc或TransTypeShard时有效。
	TransServerAddr string
	// 发现服务器URL。TransType为TransTypeNats时必须设置。
	DiscoveryUrl  string
	DiscoveryOpts kkdiscovery.DiscoveryOption
	// 集群服务器URL。TransType为TransTypeNats时可选设置。
	ClusterUrl  string
	ClusterOpts kkcluster.ClusterOption
}

func validateOption(opt *Options) error {
	if opt.TransType == transport.TransTypeRpc || opt.TransType == transport.TransTypeShard {
		if opt.TransServerAddr == "" {
			return errors.New("TransServerAddr is required")
		}
	}
	if opt.TransType == transport.TransTypeNats {
		if opt.DiscoveryUrl == "" {
			return errors.New("DiscoveryUrl is required")
		}
		if opt.ClusterUrl == "" {
			return errors.New("ClusterUrl is required")
		}
	}
	return nil
}

func DefaultOptions() Options {
	return Options{
		TransType: transport.TransTypeShard,
	}
}

func ApplyOptions(opts ...func(o *Options)) Options {
	cfg := DefaultOptions()
	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}

func WithTransType(transType transport.TransType) func(o *Options) {
	return func(o *Options) {
		o.TransType = transType
	}
}

func WithTransServerAddr(transServerAddr string) func(o *Options) {
	return func(o *Options) {
		o.TransServerAddr = transServerAddr
	}
}

func WithDiscoveryURL(natsURL string) func(o *Options) {
	return func(o *Options) {
		o.DiscoveryUrl = natsURL
	}
}

func WithClusterURL(clusterURL string) func(o *Options) {
	return func(o *Options) {
		o.ClusterUrl = clusterURL
	}
}
