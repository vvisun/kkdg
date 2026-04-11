package kkcluster

import (
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
	"github.com/vvisun/kkdg/utils/kkcodec"
	"github.com/vvisun/kkdg/utils/kklog"
)

type ClusterOption struct {
	// 发现服务（可选）。用于发送消息时检查目标节点是否在线，不在线时快速失败。
	Discovery kkdiscovery.IDiscovery
	// 消息编码器
	MsgCodec kkcodec.ICodec
	// 集群URL
	Url string
}

func DefaultClusterOption() ClusterOption {
	return ClusterOption{
		MsgCodec: kkcodec.GetCodec(kkcodec.CodecTypeJson),
		Url:      "",
	}
}

func CheckClusterOption(opt *ClusterOption) {
	if opt.MsgCodec == nil {
		kklog.Warnf("[kkcluster] msg codec is nil, use default codec: %s", "json")
		opt.MsgCodec = kkcodec.GetCodec(kkcodec.CodecTypeJson)
	}
	if opt.Url == "" {
		kklog.Warn("[kkcluster] ClusterUrl is empty, will not start cluster")
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
		o.Url = url
	}
}

func WithDiscovery(discovery kkdiscovery.IDiscovery) func(o *ClusterOption) {
	return func(o *ClusterOption) {
		o.Discovery = discovery
	}
}
