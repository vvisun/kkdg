package kkrpc

import (
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

type RpcOption struct {
	MaxPendingCount int
	StreamTool      kkpacket.IPacket
	FrameCodec      kkcodec.ICodec
	PayloadCodec    kkcodec.ICodec
}

func DefaultRpcOption() RpcOption {
	return RpcOption{
		MaxPendingCount: 1024,
		StreamTool:      kkpacket.DefaultStreamPacket(),
		FrameCodec:      kkcodec.GetCodec(kkcodec.CodecTypeMsgpack),
		PayloadCodec:    kkcodec.GetCodec(kkcodec.CodecTypeMsgpack),
	}
}

func CheckRpcOption(opt *RpcOption) {
	if opt.MaxPendingCount <= 0 {
		opt.MaxPendingCount = 1024
	}
	if opt.StreamTool == nil {
		opt.StreamTool = kkpacket.DefaultStreamPacket()
	}
	if opt.FrameCodec == nil {
		opt.FrameCodec = kkcodec.GetCodec(kkcodec.CodecTypeMsgpack)
	}
	if opt.PayloadCodec == nil {
		opt.PayloadCodec = kkcodec.GetCodec(kkcodec.CodecTypeMsgpack)
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

func WithStreamTool(streamTool kkpacket.IPacket) func(o *RpcOption) {
	return func(o *RpcOption) {
		o.StreamTool = streamTool
	}
}

func WithFrameCodec(frameCodec kkcodec.ICodec) func(o *RpcOption) {
	return func(o *RpcOption) {
		o.FrameCodec = frameCodec
	}
}

func WithPayloadCodec(payloadCodec kkcodec.ICodec) func(o *RpcOption) {
	return func(o *RpcOption) {
		o.PayloadCodec = payloadCodec
	}
}
