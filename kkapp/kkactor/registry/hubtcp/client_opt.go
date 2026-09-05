package hubtcp

import "time"

type ClientOptions struct {
	Addr     string // hubtcp server地址。
	Password string // 密码。用于认证。

	// 断线后是否自动重连。注册中心是目录服务，断开后不自愈会导致本节点的actor永久从目录中消失。
	NeedReconnect bool
	// 重连基础间隔。实际延迟按指数退避，上限为 ReconnectMaxInterval。
	ReconnectInterval time.Duration
	// 重连间隔上限。
	ReconnectMaxInterval time.Duration
	// 最大重连次数。-1 表示无限重连。
	ReconnectMaxRetries int
}

func DefaultClientOptions() ClientOptions {
	return ClientOptions{
		Addr:                 "0.0.0.0:8080",
		Password:             "123456",
		NeedReconnect:        true,
		ReconnectInterval:    time.Second,
		ReconnectMaxInterval: 4 * time.Second,
		ReconnectMaxRetries:  -1,
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

// WithClientReconnect 配置断线重连。interval <= 0 或 maxInterval <= 0 时保留默认值。
func WithClientReconnect(need bool, interval time.Duration, maxInterval time.Duration, maxRetries int) func(o *ClientOptions) {
	return func(o *ClientOptions) {
		o.NeedReconnect = need
		if interval > 0 {
			o.ReconnectInterval = interval
		}
		if maxInterval > 0 {
			o.ReconnectMaxInterval = maxInterval
		}
		o.ReconnectMaxRetries = maxRetries
	}
}
