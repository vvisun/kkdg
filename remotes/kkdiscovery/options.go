package kkdiscovery

import (
	"github.com/vvisun/kkdg/utils/kkcodec"
	"github.com/vvisun/kkdg/utils/kklog"
)

type DiscoveryOption struct {
	MsgCodec kkcodec.ICodec
}

func DefaultDiscoveryOption() DiscoveryOption {
	return DiscoveryOption{
		MsgCodec: kkcodec.GetCodec(kkcodec.CodecTypeMsgpack),
	}
}

func CheckDiscoveryOption(opt *DiscoveryOption) {
	if opt.MsgCodec == nil {
		kklog.Warnf("[kkdiscovery] msg codec is nil, use default codec: %s", "msgpack")
		opt.MsgCodec = kkcodec.GetCodec(kkcodec.CodecTypeMsgpack)
	}
}

func ApplyOptions(opts ...func(o *DiscoveryOption)) DiscoveryOption {
	cfg := DefaultDiscoveryOption()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	CheckDiscoveryOption(&cfg)
	return cfg
}

func WithMsgCodec(codec kkcodec.ICodec) func(o *DiscoveryOption) {
	return func(o *DiscoveryOption) {
		if codec == nil {
			kklog.Warnf("[kkdiscovery] msg codec is nil, use default codec: %s", "msgpack")
			codec = kkcodec.GetCodec(kkcodec.CodecTypeMsgpack)
		}
		o.MsgCodec = codec
	}
}
