package ccgame

import (
	"errors"

	"github.com/vvisun/kkdg/kkapp"
)

type Option struct {
	TransType kkapp.TransType
	RpcAddr   string
	NatsURL   string
}

func DefaultOption() Option {
	return Option{
		TransType: kkapp.TransTypeNats,
	}
}

func validateOption(opt *Option) error {
	if opt.TransType == kkapp.TransTypeRpc && opt.RpcAddr == "" {
		return errors.New("rpc addr is required")
	}
	return nil
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

func WithRpcAddr(rpcAddr string) func(o *Option) {
	return func(o *Option) {
		o.RpcAddr = rpcAddr
	}
}

func WithNatsURL(natsURL string) func(o *Option) {
	return func(o *Option) {
		o.NatsURL = natsURL
	}
}
