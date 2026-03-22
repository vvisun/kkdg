package kkdiscovery

import (
	"time"

	"github.com/vvisun/kkdg/utils/kkcodec"
	"github.com/vvisun/kkdg/utils/kklog"
)

type DiscoveryOption struct {
	MsgCodec       kkcodec.ICodec
	OfflineTimeout time.Duration
	Url            string // nats://127.0.0.1:4222
}

func DefaultDiscoveryOption() DiscoveryOption {
	return DiscoveryOption{
		MsgCodec:       kkcodec.GetCodec(kkcodec.CodecTypeJson),
		OfflineTimeout: 3 * time.Second,
		Url:            "",
	}
}

func CheckDiscoveryOption(opt *DiscoveryOption) {
	if opt.MsgCodec == nil {
		kklog.Warnf("[kkdiscovery] msg codec is nil, use default codec: %s", "json")
		opt.MsgCodec = kkcodec.GetCodec(kkcodec.CodecTypeJson)
	}
	if opt.OfflineTimeout <= 0 {
		opt.OfflineTimeout = 3 * time.Second
	}
	if opt.Url == "" {
		kklog.Warn("[kkdiscovery] DiscoveryUrl is empty, will not start discovery")
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
			kklog.Warnf("[kkdiscovery] msg codec is nil, use default codec: %s", "json")
			codec = kkcodec.GetCodec(kkcodec.CodecTypeJson)
		}
		o.MsgCodec = codec
	}
}

func WithOfflineTimeout(timeout time.Duration) func(o *DiscoveryOption) {
	return func(o *DiscoveryOption) {
		if timeout <= 0 {
			return
		}
		o.OfflineTimeout = timeout
	}
}

func WithUrl(url string) func(o *DiscoveryOption) {
	return func(o *DiscoveryOption) {
		o.Url = url
	}
}
