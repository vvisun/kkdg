package kkpacket

import "github.com/vvisun/kkdg/utils/kkcodec"

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

func (p *MessagePacket) HeadBytes(messageBytes []byte) []byte {
	return messageBytes[:p.head.GetSize()]
}

func (p *MessagePacket) BodyBytes(messageBytes []byte) []byte {
	return messageBytes[p.head.GetSize():]
}

func (p *MessagePacket) GetMsgID(messageBytes []byte) (MSGID, error) {
	headBytes := p.HeadBytes(messageBytes)
	valueList := [maxHeadPathCount]int{0}
	err := p.head.UnmarshalTo(headBytes, GetByteOrder(), valueList[:])
	if err != nil {
		return 0, err
	}
	return MSGID(valueList[0]), nil
}
