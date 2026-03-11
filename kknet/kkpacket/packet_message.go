package kkpacket

import (
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kkcodec"
	"github.com/vvisun/kkdg/utils/kklog"
)

// [message] 编码解码器。用于编码解码[message]部分。
// [message] = [head, body]
type MessagePacket struct {
	head      *PacketHead    //[head]部分编码解码器
	bodyCodec kkcodec.ICodec //[body]部分编码解码器
	router    *MsgRouter     //消息路由
}

func NewMessagePacket(head *PacketHead, bodyCodec kkcodec.ICodec, router *MsgRouter) *MessagePacket {
	if head == nil {
		panic("head is nil")
	}
	if bodyCodec == nil {
		panic("bodyCodec is nil")
	}
	if router == nil {
		panic("router is nil")
	}
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

	msgID := MSGID(vList[0])
	if p.router.GetMsgType(msgID) == nil {
		kklog.Warnf("unregistered msgID: %d", msgID)
		return msgID, kkerrors.ErrInvalidMsgID
	}

	return msgID, nil
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
