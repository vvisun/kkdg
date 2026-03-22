package kkcluster

import (
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
	"github.com/vvisun/kkdg/utils/kkcodec"
	"github.com/vvisun/kkdg/utils/kklog"
)

type ClusterOption struct {
	Discovery kkdiscovery.IDiscovery
	MsgCodec  kkcodec.ICodec
	Url       string
}

func DefaultClusterOption() ClusterOption {
	return ClusterOption{
		MsgCodec: kkcodec.GetCodec(kkcodec.CodecTypeJson),
		Url:      "nats://127.0.0.1:4222",
	}
}

func CheckClusterOption(opt *ClusterOption) {
	if opt.MsgCodec == nil {
		kklog.Warnf("[kkcluster] msg codec is nil, use default codec: %s", "json")
		opt.MsgCodec = kkcodec.GetCodec(kkcodec.CodecTypeJson)
	}
	if opt.Url == "" {
		opt.Url = "nats://127.0.0.1:4222"
	}
}

func ApplyOptions(opts ...func(o *ClusterOption)) ClusterOption {
	cfg := DefaultClusterOption()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	CheckClusterOption(&cfg)
	return cfg
}

func WithMsgCodec(codec kkcodec.ICodec) func(o *ClusterOption) {
	return func(o *ClusterOption) {
		if codec == nil {
			kklog.Warnf("[kkcluster] msg codec is nil, use default codec: %s", "json")
			codec = kkcodec.GetCodec(kkcodec.CodecTypeJson)
		}
		o.MsgCodec = codec
	}
}

func WithUrl(url string) func(o *ClusterOption) {
	return func(o *ClusterOption) {
		if url == "" {
			return
		}
		o.Url = url
	}
}

func WithDiscovery(discovery kkdiscovery.IDiscovery) func(o *ClusterOption) {
	return func(o *ClusterOption) {
		o.Discovery = discovery
	}
}
