package cnats

import (
	"time"

	"github.com/nats-io/nats.go"
)

func defaultNatsOptions() nats.Options {
	opts := nats.GetDefaultOptions()
	opts.Url = "nats://127.0.0.1:4222"
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
