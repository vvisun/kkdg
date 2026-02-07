package kkpacket

import (
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kkpool"
)

/*
解码包。
注意：外部需记得释放消息对象！！！否则消息对象得不到回收，性能反而更低！！！

	@param messageBytes []byte 包数据[message]
	@param messagePacket *MessagePacket 消息包
	@return any 消息对象（object）
	@return MSGID 消息ID
	@return error 错误
*/
func DecodeMessage(messageBytes []byte, messagePacket *MessagePacket) (any, MSGID, error) {
	msgID, body, err := ParseMsgInfo(messageBytes, messagePacket.GetHead())
	if err != nil {
		return nil, 0, err
	}

	msgType := messagePacket.GetRouter().GetMsgType(msgID)
	if msgType == nil {
		return nil, 0, kkerrors.ErrMsgIDNotRegistered
	}

	v := kkpool.GetFactoryByType(msgType).Get()
	err = messagePacket.GetBodyCodec().Unmarshal(body, v)
	if err != nil {
		kkpool.GetFactoryByType(msgType).Put(v)
		return nil, 0, kkerrors.ErrDecodeFailed
	}
	return v, msgID, nil
}

/*
*解析消息信息。

	@param messageBytes []byte 包数据[message]
	@param head *PacketHead 消息头
	@return MSGID 消息ID
	@return []byte 消息体（object的二进制数据）
	@return error 错误
*/
func ParseMsgInfo(messageBytes []byte, head *PacketHead) (MSGID, []byte, error) {
	headSize := head.GetSize()
	if headSize < 0 {
		return 0, nil, kkerrors.ErrInvalidMsgHeadType
	}
	if len(messageBytes) < headSize {
		return 0, nil, kkerrors.ErrDataTooShortToDecode
	}

	body := messageBytes[headSize:]

	valueList := [maxHeadPathCount]int{0}
	err := head.UnmarshalTo(messageBytes[:headSize], GetByteOrder(), valueList[:])
	if err != nil {
		return 0, nil, err
	}
	msgId := MSGID(valueList[0])

	return msgId, body, nil
}

/*
编码包。
注意：外部需记得释放*kkbuffer.ByteBuffer！！！否则*kkbuffer.ByteBuffer得不到回收，性能反而更低！！！

	@param v *T 消息对象（object）
	@param stream IPacket 流包类型
	@param messagePacket *MessagePacket 消息包
	@return *kkbuffer.ByteBuffer 包数据[length,message]
	@return error 错误
*/
func EncodeStream(v any, stream IPacket, messagePacket *MessagePacket) (*kkbuffer.ByteBuffer, error) {
	msgID := messagePacket.GetRouter().GetMsgID(v)
	if msgID == 0 {
		return nil, kkerrors.ErrMsgTypeNotRegistered
	}

	lfbCount := stream.LengthFieldByteCount()
	headSize := messagePacket.GetHead().GetSize()

	bb, err := messagePacket.GetBodyCodec().MarshalAppend(v, lfbCount+headSize)
	if err != nil {
		kkbuffer.Put(bb)
		return nil, kkerrors.ErrEncodeFailed
	}

	stream.writeMessageSize(bb.B, len(bb.B)-lfbCount)

	messageBytes := stream.MessageBytes(bb.B)
	err = messagePacket.GetHead().Marshal(messagePacket.HeadBytes(messageBytes), GetByteOrder(), int(msgID))
	if err != nil {
		kkbuffer.Put(bb)
		return nil, err
	}

	return bb, nil
}
