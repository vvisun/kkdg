package hubtcp

type ServerOptions struct {
	Addr     string
	Password string
}

func DefaultServerOptions() ServerOptions {
	return ServerOptions{
		Addr:     "0.0.0.0:8080",
		Password: "123456",
	}
}

func ApplyServerOptions(opts ...func(o *ServerOptions)) ServerOptions {
	cfg := DefaultServerOptions()
	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}

func WithServerAddr(addr string) func(o *ServerOptions) {
	return func(o *ServerOptions) {
		o.Addr = addr
	}
}

func WithServerPassword(password string) func(o *ServerOptions) {
	return func(o *ServerOptions) {
		o.Password = password
	}
}
