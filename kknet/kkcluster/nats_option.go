package kkcluster

import (
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
	return nats.Options{}
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
