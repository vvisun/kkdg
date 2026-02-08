package kkpacket

import (
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

// [message] 编码解码器。用于编码解码[message]部分。
type MessagePacket struct {
	head      *PacketHead
	bodyCodec kkcodec.ICodec
	router    *MsgRouter
}

func NewMessagePacket(head *PacketHead, bodyCodec kkcodec.ICodec, router *MsgRouter) *MessagePacket {
	return &MessagePacket{
		head:      head,
		bodyCodec: bodyCodec,
		router:    router,
	}
}

func (p *MessagePacket) GetHead() *PacketHead {
	return p.head
}

func (p *MessagePacket) GetBodyCodec() kkcodec.ICodec {
	return p.bodyCodec
}

func (p *MessagePacket) GetRouter() *MsgRouter {
	return p.router
}

func (p *MessagePacket) HeadBytes(messageBytes []byte) ([]byte, error) {
	if len(messageBytes) < p.head.GetSize() {
		return nil, kkerrors.ErrDataTooShortToDecode
	}
	return messageBytes[:p.head.GetSize()], nil
}

func (p *MessagePacket) BodyBytes(messageBytes []byte) ([]byte, error) {
	if len(messageBytes) < p.head.GetSize() {
		return nil, kkerrors.ErrDataTooShortToDecode
	}
	return messageBytes[p.head.GetSize():], nil
}

func (p *MessagePacket) GetMsgID(messageBytes []byte) (MSGID, error) {
	headBytes, err := p.HeadBytes(messageBytes)
	if err != nil {
		return 0, err
	}
	valueList := [maxHeadPathCount]int{0}
	vList, err := p.head.UnmarshalTo(headBytes, GetByteOrder(), valueList[:])
	if err != nil {
		return 0, err
	}
	return MSGID(vList[0]), nil
}

func (p *MessagePacket) HeadValues(messageBytes []byte, valueList []int) ([]int, error) {
	if p.head.GetPartCount() <= 0 {
		return valueList[:0], nil
	}
	headBytes, err := p.HeadBytes(messageBytes)
	if err != nil {
		return nil, err
	}
	return p.head.UnmarshalTo(headBytes, GetByteOrder(), valueList)
}
