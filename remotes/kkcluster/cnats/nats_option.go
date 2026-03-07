package cnats

import (
	"time"

	"github.com/nats-io/nats.go"
	"github.com/vvisun/kkdg/utils/kklog"
)

const (
	defaultRequestTimeout time.Duration = 2 * time.Second // request timeout default value
)

func defaultNatsOptions() nats.Options {
	opts := nats.GetDefaultOptions()
	opts.Url = "nats://127.0.0.1:4222"
	opts.RetryOnFailedConnect = true
	opts.AllowReconnect = true
	opts.MaxReconnect = -1
	opts.ReconnectWait = 2 * time.Second
	opts.Timeout = 5 * time.Second
	opts.PingInterval = 15 * time.Second
	return opts
}

func ApplyNatsOptions(options ...nats.Option) nats.Options {
	opts := defaultNatsOptions()
	for _, option := range options {
		err := option(&opts)
		if err != nil {
			kklog.Errorf("ApplyNatsOptions error: %v", err)
		}
	}
	return opts
}

func WithUrl(url string) nats.Option {
	return func(opts *nats.Options) error {
		opts.Url = url
		return nil
	}
}
