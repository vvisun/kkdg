package busnats

import (
	"time"

	"github.com/nats-io/nats.go"
)

type Option func(o *Options)

type Options struct {
	// 客户端连接地址
	// 内建客户端配置，默认为nats://127.0.0.1:4222
	Url string

	// 客户端连接超时时间
	// 内建客户端配置，默认为2s
	Timeout time.Duration

	// 前缀
	// key前缀，默认为 "kkbus:"
	TopicPrefix string

	// 客户端连接
	// 外部客户端连接配置，存在外部客户端连接时，优先使用外部客户端连接，默认为nil
	// 如果conn是外部连接，则不关闭，由外部管理。
	conn *nats.Conn
}

func defaultOptions() *Options {
	return &Options{
		Url:         "nats://127.0.0.1:4222",
		Timeout:     2 * time.Second,
		TopicPrefix: "kkbus:",
	}
}

func ApplyOptions(opts ...Option) Options {
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}
	return *o
}

// WithUrl 设置连接地址
func WithUrl(url string) Option {
	return func(o *Options) { o.Url = url }
}

// WithTimeout 客户端连接超时时间
func WithTimeout(timeout time.Duration) Option {
	return func(o *Options) { o.Timeout = timeout }
}

// WithConn 设置外部客户端连接
func WithConn(conn *nats.Conn) Option {
	return func(o *Options) { o.conn = conn }
}

// WithPrefix 设置前缀
func WithPrefix(prefix string) Option {
	return func(o *Options) { o.TopicPrefix = prefix }
}
