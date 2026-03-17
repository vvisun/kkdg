package ccgate

import (
	"errors"

	"github.com/vvisun/kkdg/kkapp/transport"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/kklog"
)

// Option configures the gate component.
type Option struct {
	TCPAddr string //客户端 tcp 连接地址
	WSAddr  string //客户端 websocket 连接地址

	MaxConnCount int //最大连接数

	// 网关与逻辑服之间的转发通道类型。
	TransType transport.TransType
	// 转发层服务器地址。TransType为TransTypeRpc或TransTypeShard时有效。
	TransServerAddr string

	// 逻辑服节点类型。默认值为"logic"。
	LogicNodeType string

	// 发现服务器URL。
	DiscoveryUrl string
	// 集群服务器URL。
	ClusterUrl string

	// 接收队列满回调。
	// 可以考虑限流/提示服务器繁忙等。如：限流则通知客户端，提示服务器繁忙则提示客户端稍后再试。
	RecvQueueFullCallback func(conn kknet.IConn)
	// 分配逻辑服失败回调。如：分配失败则通知客户端，提示服务器繁忙则提示客户端稍后再试。
	AllocLogicNodeFailedCallback func(conn kknet.IConn)
	// 用户被顶号/被踢出会话回调。需要投递给业务回调，发送顶号消息给被踢的连接。
	UserKickedCallback func(conn kknet.IConn)
}

func DefaultOption() Option {
	return Option{
		MaxConnCount: 50000,
		TransType:    transport.TransTypeShard,
	}
}

func ApplyOption(opt *Option, opts ...func(o *Option)) *Option {
	for _, o := range opts {
		o(opt)
	}
	return opt
}

func validateOption(opt *Option) error {
	if opt.MaxConnCount <= 0 {
		opt.MaxConnCount = 50000
		kklog.Warn("max conn count is required, set to 50000")
	}
	if opt.TCPAddr == "" && opt.WSAddr == "" {
		return errors.New("tcp addr or ws addr is required")
	}
	if opt.TransType == transport.TransTypeRpc || opt.TransType == transport.TransTypeShard {
		if opt.TransServerAddr == "" {
			return errors.New("rpc addr is required")
		}
	}
	if opt.DiscoveryUrl == "" {
		return errors.New("discovery url is required")
	}
	if opt.ClusterUrl == "" {
		return errors.New("cluster url is required")
	}
	if opt.TCPAddr == opt.WSAddr || opt.TCPAddr == opt.TransServerAddr || opt.WSAddr == opt.TransServerAddr {
		return errors.New("tcp addr, ws addr and rpc addr cannot be the same")
	}
	if opt.LogicNodeType == "" {
		return errors.New("logic node type is required")
	}
	return nil
}

func WithTransType(transType transport.TransType) func(o *Option) {
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

func WithDiscoveryURL(natsURL string) func(o *Option) {
	return func(o *Option) {
		o.DiscoveryUrl = natsURL
	}
}

func WithClusterURL(clusterURL string) func(o *Option) {
	return func(o *Option) {
		o.ClusterUrl = clusterURL
	}
}

func WithLogicNodeType(logicNodeType string) func(o *Option) {
	return func(o *Option) {
		o.LogicNodeType = logicNodeType
	}
}

func WithTransServerAddr(transServerAddr string) func(o *Option) {
	return func(o *Option) {
		o.TransServerAddr = transServerAddr
	}
}

func WithMaxConnCount(maxConnCount int) func(o *Option) {
	return func(o *Option) {
		o.MaxConnCount = maxConnCount
	}
}

func WithRecvQueueFullCallback(callback func(conn kknet.IConn)) func(o *Option) {
	return func(o *Option) {
		o.RecvQueueFullCallback = callback
	}
}

func WithAllocLogicNodeFailedCallback(callback func(conn kknet.IConn)) func(o *Option) {
	return func(o *Option) {
		o.AllocLogicNodeFailedCallback = callback
	}
}

func WithUserKickedCallback(callback func(conn kknet.IConn)) func(o *Option) {
	return func(o *Option) {
		o.UserKickedCallback = callback
	}
}
