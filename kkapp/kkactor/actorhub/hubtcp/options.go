package hubtcp

type Options struct {
	Addr        string
	Password    string
	clientCount int
}

func DefaultOptions() Options {
	return Options{
		Addr:        "0.0.0.0:8080",
		Password:    "123456",
		clientCount: 1,
	}
}

func ApplyOptions(opts ...func(o *Options)) Options {
	cfg := DefaultOptions()
	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}

func WithAddr(addr string) func(o *Options) {
	return func(o *Options) {
		o.Addr = addr
	}
}

func WithPassword(password string) func(o *Options) {
	return func(o *Options) {
		o.Password = password
	}
}

func WithClientCount(clientCount int) func(o *Options) {
	return func(o *Options) {
		o.clientCount = clientCount
	}
}
