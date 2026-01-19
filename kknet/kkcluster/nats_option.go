package kkcluster

import (
	"time"

	"github.com/nats-io/nats.go"
)

var (
	defaultNatsAddress = "nats://127.0.0.1:4222"
)

type NatsOption func(*nats.Options)

func SetNatsAddress(address string) {
	defaultNatsAddress = address
}

func DefaultNatsOptions() nats.Options {
	return nats.Options{
		Url:            defaultNatsAddress,
		AllowReconnect: true,
		MaxReconnect:   -1,
		ReconnectWait:  2 * time.Second,
		Timeout:        5 * time.Second,
	}
}

func ApplyNatsOptions(options ...NatsOption) nats.Options {
	opts := DefaultNatsOptions()
	for _, option := range options {
		option(&opts)
	}
	return opts
}

func WithNatsOption(option NatsOption) NatsOption {
	return func(options *nats.Options) {
		option(options)
	}
}
