package dnats

import (
	"time"

	"github.com/nats-io/nats.go"
	"github.com/vvisun/kkdg/utils/kkcodec"
	"github.com/vvisun/kkdg/utils/kklog"
)

const (
	// publish self interval.
	// 每隔多长时间发布一次自己的信息
	defaultPublishSelfInterval time.Duration = 5 * time.Second
	// request all members interval
	// 检查成员超时间隔. 每隔多长时间检查一次成员是否超时
	defaultCheckMemberInterval time.Duration = 10 * time.Second
	// member timeout
	// 成员超时时间. 超过多长时间没有收到该成员的更新信息时，认为该成员已离线
	defaultMemberTimeout time.Duration = 15 * time.Second
)

var msgCodec = kkcodec.GetCodec(kkcodec.CodecTypeMsgpack)

func SetMsgCodec(codec kkcodec.ICodec) {
	if codec == nil {
		kklog.Errorf("[kkcluster] SetMsgCodec codec is nil, use default codec")
		codec = kkcodec.GetCodec(kkcodec.CodecTypeMsgpack)
	}
	msgCodec = codec
}

func defaultNatsOptions() nats.Options {
	opts := nats.GetDefaultOptions()
	opts.Url = "nats://127.0.0.1:4222"
	opts.RetryOnFailedConnect = true
	opts.AllowReconnect = true
	opts.MaxReconnect = -1
	opts.ReconnectWait = 2 * time.Second
	opts.Timeout = 5 * time.Second
	return opts
}

func ApplyNatsOptions(options ...nats.Option) nats.Options {
	opts := defaultNatsOptions()
	for _, option := range options {
		option(&opts)
	}
	return opts
}

func WithUrl(url string) nats.Option {
	return func(opts *nats.Options) error {
		opts.Url = url
		return nil
	}
}
