package ccgame

import (
	"errors"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/transport"
	"github.com/vvisun/kkdg/remotes/kkcluster"
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/xos"
)

type Options struct {
	// 网关与业务服之间的转发通道类型。
	TransType transport.TransType
	// 转发层服务器地址。TransType为TransTypeRpc或TransTypeShard时有效。
	TransServerAddr string
	// 发现服务器URL。TransType为TransTypeNats时必须设置。
	DiscoveryOpts kkdiscovery.DiscoveryOption
	// 集群服务器URL。TransType为TransTypeNats时可选设置。
	ClusterOpts kkcluster.ClusterOption
	// 会话管理器工作线程数量。会创建多个工作线程来解码消息。
	SessionManagerWorkersCount int
	// 用于game服的会话消息接收器中。gametrans.ISessionMsgReceiver.OnSession中使用。
	// 解码错误或消息ID不存在时，是否抛给上层处理。
	// 上层可以返回一个错误码给客户端，然后关闭连接，这样即对客户端友好，又能防止恶意攻击。
	DecodeErrorCallback kkapp.DecodeErrorCallback
	// 用于客户端消息接收器中。网关并不对消息进行完整解码（只关心消息ID和消息路由），所以该回调可以不设置。
	// 解码错误或消息ID不存在时，是否抛给上层处理。
	// 上层可以返回一个错误码给客户端，然后关闭连接，这样即对客户端友好，又能防止恶意攻击。
	DecodeErrorCallbackOnRaw kkapp.DecodeErrorCallbackOnRaw
}

func validateOption(opt *Options) error {
	if opt.TransType == transport.TransTypeRpc || opt.TransType == transport.TransTypeShard {
		if opt.TransServerAddr == "" {
			return errors.New("TransServerAddr is required")
		}
	}
	if opt.TransType == transport.TransTypeNats {
		if opt.DiscoveryOpts.Url == "" {
			kklog.Warn("DiscoveryUrl is required") //非必需。
		}
		if opt.ClusterOpts.Url == "" {
			return errors.New("ClusterUrl is required")
		}
	}
	if opt.SessionManagerWorkersCount <= 0 {
		opt.SessionManagerWorkersCount = xos.NumCPU() * 4
	}
	return nil
}

func DefaultOptions() Options {
	return Options{
		TransType:                  transport.TransTypeShard,
		SessionManagerWorkersCount: xos.NumCPU() * 4,
	}
}

func ApplyOptions(opts ...func(o *Options)) Options {
	cfg := DefaultOptions()
	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}

func WithTransType(transType transport.TransType) func(o *Options) {
	return func(o *Options) {
		o.TransType = transType
	}
}

func WithTransServerAddr(transServerAddr string) func(o *Options) {
	return func(o *Options) {
		o.TransServerAddr = transServerAddr
	}
}

func WithDiscoveryOpts(opts kkdiscovery.DiscoveryOption) func(o *Options) {
	return func(o *Options) {
		o.DiscoveryOpts = opts
	}
}

func WithClusterOpts(opts kkcluster.ClusterOption) func(o *Options) {
	return func(o *Options) {
		o.ClusterOpts = opts
	}
}

func WithDecodeErrorCallback(callback kkapp.DecodeErrorCallback) func(o *Options) {
	return func(o *Options) {
		o.DecodeErrorCallback = callback
	}
}

func WithDecodeErrorCallbackOnRaw(callback kkapp.DecodeErrorCallbackOnRaw) func(o *Options) {
	return func(o *Options) {
		o.DecodeErrorCallbackOnRaw = callback
	}
}
