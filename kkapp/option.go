package kkapp

import (
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/kkcodec"
	"github.com/vvisun/kkdg/utils/kklog"
)

type AppOptions struct {
	StreamTool      kkpacket.IPacket
	ClientMsgPacket *kkpacket.MessagePacket
	TransMsgPacket  *kkpacket.MessagePacket
}

func DefaultOptions() AppOptions {
	return AppOptions{
		StreamTool: kkpacket.DefaultStreamPacket(),
		ClientMsgPacket: kkpacket.NewMessagePacket(
			kkpacket.NewPacketHead(&kkpacket.PartUint32{}),
			kkcodec.GetCodec(kkcodec.CodecTypeJson),
			kkpacket.NewMsgRouter(),
		),
		TransMsgPacket: kkpacket.NewMessagePacket(
			kkpacket.NewPacketHead(&kkpacket.PartUint32{}),
			kkcodec.GetCodec(kkcodec.CodecTypeMsgpack),
			kkpacket.NewMsgRouter(),
		),
	}
}

func CheckOptions(opt *AppOptions) {
	if opt == nil {
		return
	}
	if opt.StreamTool == nil {
		kklog.Warnf("[kkapp] stream tool is nil, use default stream tool")
		opt.StreamTool = kkpacket.DefaultStreamPacket()
	}
	if opt.ClientMsgPacket == nil {
		kklog.Warnf("[kkapp] client msg packet is nil, use default client msg packet")
		opt.ClientMsgPacket = kkpacket.NewMessagePacket(
			kkpacket.NewPacketHead(&kkpacket.PartUint32{}),
			kkcodec.GetCodec(kkcodec.CodecTypeJson),
			kkpacket.NewMsgRouter(),
		)
	}
	if opt.TransMsgPacket == nil {
		kklog.Warnf("[kkapp] trans msg packet is nil, use default trans msg packet")
		opt.TransMsgPacket = kkpacket.NewMessagePacket(
			kkpacket.NewPacketHead(&kkpacket.PartUint32{}),
			kkcodec.GetCodec(kkcodec.CodecTypeMsgpack),
			kkpacket.NewMsgRouter(),
		)
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
		if streamTool == nil {
			return
		}
		o.StreamTool = streamTool
	}
}

func WithClientMsgPacket(clientMsgPacket *kkpacket.MessagePacket) func(o *AppOptions) {
	return func(o *AppOptions) {
		if clientMsgPacket == nil {
			return
		}
		o.ClientMsgPacket = clientMsgPacket
	}
}

func WithTransMsgPacket(transMsgPacket *kkpacket.MessagePacket) func(o *AppOptions) {
	return func(o *AppOptions) {
		if transMsgPacket == nil {
			return
		}
		o.TransMsgPacket = transMsgPacket
	}
}
