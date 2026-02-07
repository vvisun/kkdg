package kkpacket

import (
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kkcodec"
	"github.com/vvisun/kkdg/utils/kkpool"
)

/*
* 解析消息信息。

	@param data []byte 包数据[message]
	@param pkType *PacketCodec 包类型
	@return MSGID 消息ID
	@return []byte 消息体（object的二进制数据）
	@return error 错误
*/
func ParseMsgInfo(data []byte, head *PacketHead) (MSGID, []byte, error) {
	headSize := head.GetSize()
	if headSize < 0 {
		return 0, nil, kkerrors.ErrInvalidMsgHeadType
	}
	if len(data) < headSize {
		return 0, nil, kkerrors.ErrDataTooShortToDecode
	}

	body := data[headSize:]

	valueList := [max_head_part_count]int{0}
	err := head.UnmarshalTo(data[:headSize], GetByteOrder(), valueList[:])
	if err != nil {
		return 0, nil, err
	}
	msgId := MSGID(valueList[0])

	return msgId, body, nil
}

/*
解码包。
注意：外部需记得释放消息对象！！！否则消息对象得不到回收，性能反而更低！！！

	@param data []byte 包数据[message]
	@param pkType *PacketCodec 包类型
	@return any 消息对象（object）
	@return MSGID 消息ID
	@return error 错误
*/
func DecodePacket(data []byte, head *PacketHead, bodyCodec kkcodec.ICodec, router *MsgRouter) (any, MSGID, error) {
	msgID, body, err := ParseMsgInfo(data, head)
	if err != nil {
		return nil, 0, err
	}

	msgType := router.GetMsgType(msgID)
	if msgType == nil {
		return nil, 0, kkerrors.ErrMsgIDNotRegistered
	}

	v := kkpool.GetFactoryByType(msgType).Get()
	err = bodyCodec.Unmarshal(body, v)
	if err != nil {
		kkpool.GetFactoryByType(msgType).Put(v)
		return nil, 0, kkerrors.ErrDecodeFailed
	}
	return v, msgID, nil
}

/*
编码包。
注意：外部需记得释放缓冲区！！！否则缓冲区得不到回收，性能反而更低！！！

	@param v *T 消息对象（object）
	@param pkType *PacketCodec 包类型
	@return *kkbuffer.ByteBuffer 包数据[message]
	@return error 错误
*/
func EncodePacket[T any](v *T, head *PacketHead, bodyCodec kkcodec.ICodec, router *MsgRouter) (*kkbuffer.ByteBuffer, error) {
	headSize := head.GetSize()

	msgID := router.GetMsgID(v)
	if msgID == 0 {
		return nil, kkerrors.ErrMsgTypeNotRegistered
	}

	buf, err := bodyCodec.MarshalAppend(v, headSize)
	if err != nil {
		return nil, kkerrors.ErrEncodeFailed
	}

	endian := GetByteOrder()
	valueList := [max_head_part_count]int{0}
	err = head.UnmarshalTo(buf.B[:headSize], endian, valueList[:])
	if err != nil {
		return nil, err
	}

	return buf, nil
}

/*
编码包。
注意：外部需记得释放*kkbuffer.ByteBuffer！！！否则*kkbuffer.ByteBuffer得不到回收，性能反而更低！！！

	@param v *T 消息对象（object）
	@param stream IStreamPacket 流包类型
	@return *kkbuffer.ByteBuffer 包数据[length,message]
	@return error 错误
*/
func EncodeStream(v any, stream IPacket, router *MsgRouter) (*kkbuffer.ByteBuffer, error) {
	headSize := stream.GetHead().GetSize()

	msgID := router.GetMsgID(v)
	if msgID == 0 {
		return nil, kkerrors.ErrMsgTypeNotRegistered
	}

	lfbCount := stream.LengthFieldByteCount()

	bb, err := stream.GetBodyCodec().MarshalAppend(v, lfbCount+headSize)
	if err != nil {
		kkbuffer.Put(bb)
		return nil, kkerrors.ErrEncodeFailed
	}

	if len(bb.B)-lfbCount-headSize < 0 {
		kkbuffer.Put(bb)
		return nil, kkerrors.ErrInvalidMsgHeadType
	}

	stream.writeMessageSize(bb.B[:lfbCount], len(bb.B)-lfbCount)

	err = stream.GetHead().Marshal(stream.HeadBytes(bb.B), GetByteOrder(), int(msgID))
	if err != nil {
		kkbuffer.Put(bb)
		return nil, err
	}

	return bb, nil
}
