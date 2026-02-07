package kkpacket

import "github.com/vvisun/kkdg/utils/kkcodec"

// [message] = [head,body]
// head = [msgID,seq,....]
// body = [object(binary data of object)]
type PacketCodec struct {
	headType HeadType       // 消息头[head]类型。
	codec    kkcodec.ICodec // 消息体[body]编码器。
}

// new message codec.
// @param headType HeadType 消息头[head]类型。
// @param codec kkcodec.ICodec 消息体[body]编码器。
// @return *PacketCodec 消息[message]编码器。
func NewPacketCodec(headType HeadType, codec kkcodec.ICodec) *PacketCodec {
	return &PacketCodec{
		headType: headType,
		codec:    codec,
	}
}
