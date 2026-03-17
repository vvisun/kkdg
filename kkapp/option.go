package kkapp

import (
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/kkcodec"
	"github.com/vvisun/kkdg/utils/kklog"
)

type AppOptions struct {
	// 理论上可以分别设置transport和client的stream工具。
	// 但为了简化配置，直接都使用同一个了，影响不大，就是一个[length]字段用2字节还是4字节的问题而已。
	StreamTool kkpacket.IPacket
	// 客户端与服务器之间的协议约定。
	// ClientMsgPacket.router外部应该为他注册消息。否则客户端发来消息时，找不到对应的编解码器。
	ClientMsgPacket *kkpacket.MessagePacket
	// 转发层的消息编解码器。
	TransportorCodec kkcodec.ICodec
}

func DefaultOptions() AppOptions {
	return AppOptions{
		StreamTool: kkpacket.DefaultStreamPacket(),
		ClientMsgPacket: kkpacket.NewMessagePacket(
			kkpacket.NewPacketHead(&kkpacket.PartUint32{}),
			kkcodec.GetCodec(kkcodec.CodecTypeJson),
			kkpacket.NewMsgRouter(),
		),
		TransportorCodec: kkcodec.GetCodec(kkcodec.CodecTypeMsgpack),
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
	if opt.TransportorCodec == nil {
		kklog.Warnf("[kkapp] trans msg packet is nil, use default trans msg packet")
		opt.TransportorCodec = kkcodec.GetCodec(kkcodec.CodecTypeMsgpack)
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

func WithTransportorCodec(codec kkcodec.ICodec) func(o *AppOptions) {
	return func(o *AppOptions) {
		o.TransportorCodec = codec
	}
}
