package ccgate

import (
	"errors"

	"github.com/vvisun/kkdg/kkapp"
)

// Option configures the gate component.
type Option struct {
	TCPAddr string
	WSAddr  string
	RpcAddr string

	// NatsURL is the NATS server url used by discovery/cluster.
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

func validateOption(opt *Option) error {
	if opt.TCPAddr == "" && opt.WSAddr == "" {
		return errors.New("tcp addr or ws addr is required")
	}
	if opt.TransType == kkapp.TransTypeRpc && opt.RpcAddr == "" {
		return errors.New("rpc addr is required")
	}
	if opt.TCPAddr == opt.WSAddr || opt.TCPAddr == opt.RpcAddr || opt.WSAddr == opt.RpcAddr {
		return errors.New("tcp addr, ws addr and rpc addr cannot be the same")
	}
	return nil
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

func WithRpcAddr(rpcAddr string) func(o *Option) {
	return func(o *Option) {
		o.RpcAddr = rpcAddr
	}
}
