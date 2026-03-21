package hubtcp

type ClientOptions struct {
	Addr     string
	Password string
}

func DefaultClientOptions() ClientOptions {
	return ClientOptions{
		Addr:     "0.0.0.0:8080",
		Password: "123456",
	}
}

func ApplyClientOptions(opts ...func(o *ClientOptions)) ClientOptions {
	cfg := DefaultClientOptions()
	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}

func WithAddr(addr string) func(o *ClientOptions) {
	return func(o *ClientOptions) {
		o.Addr = addr
	}
}

func WithPassword(password string) func(o *ClientOptions) {
	return func(o *ClientOptions) {
		o.Password = password
	}
}
