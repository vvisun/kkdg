package nats

import (
	"time"

	"github.com/nats-io/nats.go"
)

const (
	defaultUrl     = "nats://127.0.0.1:4222"
	defaultTimeout = 2 * time.Second
	defaultPrefix  = "kkbus:"
)

type Option func(o *options)

type options struct {
	// 客户端连接地址
	// 内建客户端配置，默认为nats://127.0.0.1:4222
	url string

	// 客户端连接超时时间
	// 内建客户端配置，默认为2s
	timeout time.Duration

	// 客户端连接
	// 外部客户端连接配置，存在外部客户端连接时，优先使用外部客户端连接，默认为nil
	conn *nats.Conn

	// 前缀
	// key前缀，默认为 "kkbus:"
	prefix string
}

func defaultOptions() *options {
	return &options{
		url:     defaultUrl,
		timeout: defaultTimeout,
		prefix:  defaultPrefix,
	}
}

// WithUrl 设置连接地址
func WithUrl(url string) Option {
	return func(o *options) { o.url = url }
}

// WithTimeout 客户端连接超时时间
func WithTimeout(timeout time.Duration) Option {
	return func(o *options) { o.timeout = timeout }
}

// WithConn 设置外部客户端连接
func WithConn(conn *nats.Conn) Option {
	return func(o *options) { o.conn = conn }
}

// WithPrefix 设置前缀
func WithPrefix(prefix string) Option {
	return func(o *options) { o.prefix = prefix }
}
