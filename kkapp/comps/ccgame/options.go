package ccgame

import "github.com/vvisun/kkdg/kkapp"

type Option struct {
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
