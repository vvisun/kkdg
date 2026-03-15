package kkrpc

type RpcOption struct {
	MaxPendingCount int
}

func DefaultRpcOption() RpcOption {
	return RpcOption{
		MaxPendingCount: 1024 * 8,
	}
}

func CheckRpcOption(opt *RpcOption) {
	if opt.MaxPendingCount <= 0 {
		opt.MaxPendingCount = 8192
	}
}

func ApplyOptions(opts ...func(o *RpcOption)) RpcOption {
	cfg := DefaultRpcOption()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}

func WithMaxPendingCount(maxPendingCount int) func(o *RpcOption) {
	return func(o *RpcOption) {
		o.MaxPendingCount = maxPendingCount
	}
}
