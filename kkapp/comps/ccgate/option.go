package ccgate

import "github.com/vvisun/kkdg/kkapp"

// Option configures the gate component.
type Option struct {
	TCPAddr string
	WSAddr  string

	// NatsURL is the NATS server url used by discovery/cluster.
	// If empty, it falls back to nodeInfo setting "nats_url", then default "nats://127.0.0.1:4222".
	NatsURL string

	// LogicNodeType is the target node type for game logic nodes.
	// If empty, defaults to "logic".
	LogicNodeType string

	TransType kkapp.TransType
}

func DefaultOption() Option {
	return Option{
		TransType: kkapp.TransTypeNats,
	}
}

func ApplyOption(opt *Option, opts ...func(o *Option)) *Option {
	for _, o := range opts {
		o(opt)
	}
	return opt
}

func WithTransType(transType kkapp.TransType) func(o *Option) {
	return func(o *Option) {
		o.TransType = transType
	}
}

func WithTCPAddr(tcpAddr string) func(o *Option) {
	return func(o *Option) {
		o.TCPAddr = tcpAddr
	}
}

func WithWSAddr(wsAddr string) func(o *Option) {
	return func(o *Option) {
		o.WSAddr = wsAddr
	}
}

func WithNatsURL(natsURL string) func(o *Option) {
	return func(o *Option) {
		o.NatsURL = natsURL
	}
}

func WithLogicNodeType(logicNodeType string) func(o *Option) {
	return func(o *Option) {
		o.LogicNodeType = logicNodeType
	}
}
