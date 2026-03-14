package kkapp

import (
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/kklog"
)

type AppOptions struct {
	StreamTool      kkpacket.IPacket
	ClientMsgPacket *kkpacket.MessagePacket
	TransMsgPacket  *kkpacket.MessagePacket
}

func DefaultOptions() AppOptions {
	return AppOptions{
		StreamTool:      gStreamTool,
		ClientMsgPacket: gClientMsgPacket,
		TransMsgPacket:  gTransMsgPacket,
	}
}

func CheckOptions(opt *AppOptions) {
	if opt == nil {
		return
	}
	if opt.StreamTool == nil {
		kklog.Warnf("[kkapp] stream tool is nil, use default stream tool")
		opt.StreamTool = gStreamTool
	}
	if opt.ClientMsgPacket == nil {
		kklog.Warnf("[kkapp] client msg packet is nil, use default client msg packet")
		opt.ClientMsgPacket = gClientMsgPacket
	}
	if opt.TransMsgPacket == nil {
		kklog.Warnf("[kkapp] trans msg packet is nil, use default trans msg packet")
		opt.TransMsgPacket = gTransMsgPacket
	}
}

func ApplyOptions(opts ...func(o *AppOptions)) AppOptions {
	cfg := DefaultOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	CheckOptions(&cfg)
	return cfg
}

func WithStreamTool(streamTool kkpacket.IPacket) func(o *AppOptions) {
	return func(o *AppOptions) {
		o.StreamTool = streamTool
	}
}

func WithClientMsgPacket(clientMsgPacket *kkpacket.MessagePacket) func(o *AppOptions) {
	return func(o *AppOptions) {
		o.ClientMsgPacket = clientMsgPacket
	}
}

func WithTransMsgPacket(transMsgPacket *kkpacket.MessagePacket) func(o *AppOptions) {
	return func(o *AppOptions) {
		o.TransMsgPacket = transMsgPacket
	}
}
