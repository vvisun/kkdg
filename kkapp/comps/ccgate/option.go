package ccgate

import (
	"errors"

	"github.com/vvisun/kkdg/kkapp/transport"
	"github.com/vvisun/kkdg/remotes/kkcluster"
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
	"github.com/vvisun/kkdg/utils/kklog"
)

// Options configures the gate component.
type Options struct {
	TCPAddr string //客户端 tcp 连接地址
	WSAddr  string //客户端 websocket 连接地址

	MaxConnCount int //最大连接数

	// 网关与业务服之间的转发通道类型。
	TransType transport.TransType
	// 转发层服务器地址。TransType为TransTypeRpc或TransTypeShard时有效。
	TransServerAddr string

	// 发现服务器URL。
	DiscoveryUrl  string
	DiscoveryOpts kkdiscovery.DiscoveryOption
	// 集群服务器URL。
	ClusterUrl  string
	ClusterOpts kkcluster.ClusterOption
}

func validateOption(opt *Options) error {
	if opt.MaxConnCount <= 0 {
		opt.MaxConnCount = 50000
		kklog.Warn("max conn count is required, set to 50000")
	}
	if opt.TCPAddr == "" && opt.WSAddr == "" {
		return errors.New("tcp addr or ws addr is required")
	}
	if opt.TransType == transport.TransTypeRpc || opt.TransType == transport.TransTypeShard {
		if opt.TransServerAddr == "" {
			return errors.New("rpc addr is required")
		}
	}
	if opt.DiscoveryUrl == "" {
		return errors.New("discovery url is required")
	}
	if opt.ClusterUrl == "" {
		return errors.New("cluster url is required")
	}
	if opt.TCPAddr == opt.WSAddr || opt.TCPAddr == opt.TransServerAddr || opt.WSAddr == opt.TransServerAddr {
		return errors.New("tcp addr, ws addr and rpc addr cannot be the same")
	}
	return nil
}

func DefaultOptions() Options {
	return Options{
		MaxConnCount: 50000,
		TransType:    transport.TransTypeShard,
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

func WithTCPAddr(tcpAddr string) func(o *Options) {
	return func(o *Options) {
		o.TCPAddr = tcpAddr
	}
}

func WithWSAddr(wsAddr string) func(o *Options) {
	return func(o *Options) {
		o.WSAddr = wsAddr
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

func WithTransServerAddr(transServerAddr string) func(o *Options) {
	return func(o *Options) {
		o.TransServerAddr = transServerAddr
	}
}

func WithMaxConnCount(maxConnCount int) func(o *Options) {
	return func(o *Options) {
		o.MaxConnCount = maxConnCount
	}
}
