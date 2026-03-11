package kkcluster

import (
	"github.com/vvisun/kkdg/utils/kkcodec"
	"github.com/vvisun/kkdg/utils/kklog"
)

type ClusterOption struct {
	MsgCodec kkcodec.ICodec
}

func DefaultClusterOption() ClusterOption {
	return ClusterOption{
		MsgCodec: kkcodec.GetCodec(kkcodec.CodecTypeMsgpack),
	}
}

func CheckClusterOption(opt *ClusterOption) {
	if opt.MsgCodec == nil {
		kklog.Warnf("[kkcluster] msg codec is nil, use default codec: %s", "msgpack")
		opt.MsgCodec = kkcodec.GetCodec(kkcodec.CodecTypeMsgpack)
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
			kklog.Warnf("[kkcluster] msg codec is nil, use default codec: %s", "msgpack")
			codec = kkcodec.GetCodec(kkcodec.CodecTypeMsgpack)
		}
		o.MsgCodec = codec
	}
}
