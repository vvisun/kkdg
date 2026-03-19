package kkdiscovery

import (
	"time"

	"github.com/vvisun/kkdg/utils/kkcodec"
	"github.com/vvisun/kkdg/utils/kklog"
)

type DiscoveryOption struct {
	MsgCodec       kkcodec.ICodec
	OfflineTimeout time.Duration
	Url            string
}

func DefaultDiscoveryOption() DiscoveryOption {
	return DiscoveryOption{
		MsgCodec:       kkcodec.GetCodec(kkcodec.CodecTypeMsgpack),
		OfflineTimeout: 3 * time.Second,
		Url:            "nats://127.0.0.1:4222",
	}
}

func CheckDiscoveryOption(opt *DiscoveryOption) {
	if opt.MsgCodec == nil {
		kklog.Warnf("[kkdiscovery] msg codec is nil, use default codec: %s", "msgpack")
		opt.MsgCodec = kkcodec.GetCodec(kkcodec.CodecTypeMsgpack)
	}
	if opt.OfflineTimeout <= 0 {
		opt.OfflineTimeout = 3 * time.Second
	}
	if opt.Url == "" {
		opt.Url = "nats://127.0.0.1:4222"
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
		if url == "" {
			return
		}
		o.Url = url
	}
}
