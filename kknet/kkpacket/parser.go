package kkpacket

import (
	"encoding/binary"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kkcodec"
	"github.com/vvisun/kkdg/utils/kkpool"
)

const (
	HeadTypeMid uint8 = iota
	HeadTypeMidSeq
)

func GetHeadSize(headType uint8) int {
	switch headType {
	case HeadTypeMid:
		return 4
	case HeadTypeMidSeq:
		return 8
	default:
		return -1
	}
}

type HeadMid struct {
	mid uint32
}

type HeadMidSeq struct {
	mid uint32
	seq uint32
}

func ParseHeadMid(data []byte, endian binary.ByteOrder) HeadMid {
	return HeadMid{
		mid: endian.Uint32(data[:4]),
	}
}

func ParseHeadMidSeq(data []byte, endian binary.ByteOrder) HeadMidSeq {
	return HeadMidSeq{
		mid: endian.Uint32(data[:4]),
		seq: endian.Uint32(data[4:8]),
	}
}

type MsgInfo struct {
	MsgId MSGID
	Data  []byte
	Err   error
}

/*
* 解析消息信息。

	@param data []byte 包数据[message]
	@param pkType *PacketCodec 包类型
	@return MSGID 消息ID
	@return []byte 消息体（object的二进制数据）
	@return error 错误
*/
func ParseMsgInfo(data []byte, pkType *PacketCodec) (MSGID, []byte, error) {
	headSize := GetHeadSize(pkType.headType)
	if headSize < 0 {
		return 0, nil, kkerrors.ErrInvalidMsgHeadType
	}
	if len(data) < headSize {
		return 0, nil, kkerrors.ErrDataTooShortToDecode
	}

	msgId := uint32(0)
	body := data[headSize:]

	switch pkType.headType {
	case HeadTypeMid:
		head := ParseHeadMid(data[:headSize], GetByteOrder())
		msgId = head.mid
	case HeadTypeMidSeq:
		head := ParseHeadMidSeq(data[:headSize], GetByteOrder())
		msgId = head.mid
	}
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
func DecodePacket(data []byte, pkType *PacketCodec) (any, MSGID, error) {
	codec := kkcodec.GetCodec(pkType.codecType)
	if codec == nil {
		return nil, 0, kkerrors.ErrInvalidCodec
	}

	msgID, body, err := ParseMsgInfo(data, pkType)
	if err != nil {
		return nil, 0, err
	}

	msgType := GetMsgType(msgID)
	if msgType == nil {
		return nil, 0, kkerrors.ErrMsgIDNotRegistered
	}

	v := kkpool.GetFactoryByType(msgType).Get()
	err = codec.Unmarshal(body, v)
	if err != nil {
		kkpool.GetFactoryByType(msgType).Put(v)
		return nil, 0, kkerrors.ErrDecodeFailed
	}
	return v, msgID, nil
}

/*
编码包。

	@param v *T 消息类型
	@param pkType *PacketCodec 包类型
	@return []byte 包数据[message]
	@return error 错误
*/
func EncodePacket[T any](v *T, pkType *PacketCodec) ([]byte, error) {
	buf, err := EncodePacketEx(v, pkType)
	if err != nil {
		return nil, err
	}
	if len(buf.B) == 0 {
		kkbuffer.Put(buf)
		return make([]byte, 0), nil
	}
	return buf.B, nil
}

/*
编码包。
注意：外部需记得释放缓冲区！！！否则缓冲区得不到回收，性能反而更低！！！

	@param v *T 消息对象（object）
	@param pkType *PacketCodec 包类型
	@return buffers.IBuffer 包数据[message]
	@return error 错误
*/
func EncodePacketEx[T any](v *T, pkType *PacketCodec) (buffers.IBuffer, error) {
	codec := kkcodec.GetCodec(pkType.codecType)
	if codec == nil {
		return nil, kkerrors.ErrInvalidCodec
	}

	headSize := GetHeadSize(pkType.headType)
	if headSize < 0 {
		return nil, kkerrors.ErrInvalidMsgHeadType
	}

	msgID := GetMsgID(v)
	if msgID == 0 {
		return nil, kkerrors.ErrMsgTypeNotRegistered
	}

	buf, err := codec.MarshalAppend(v, headSize)
	if err != nil {
		return nil, kkerrors.ErrEncodeFailed
	}

	endian := GetByteOrder()
	switch pkType.headType {
	case HeadTypeMid:
		endian.PutUint32(buf.B[:4], msgID)
	case HeadTypeMidSeq:
		seq := uint32(0)
		endian.PutUint32(buf.B[:4], msgID)
		endian.PutUint32(buf.B[4:8], seq)
	default:
		kkbuffer.Put(buf)
		return nil, kkerrors.ErrInvalidMsgHeadType
	}

	return buf, nil
}

/*
编码包。
注意：外部需记得释放缓冲区！！！否则缓冲区得不到回收，性能反而更低！！！

	@param v *T 消息对象（object）
	@param stream IStreamPacket 流包类型
	@return buffers.IBuffer 包数据[length,message]
	@return error 错误
*/
func EncodeStream(v any, stream IStreamPacket) (buffers.IBuffer, error) {
	pkType := stream.GetMessagePacket()
	codec := kkcodec.GetCodec(pkType.codecType)
	if codec == nil {
		return nil, kkerrors.ErrInvalidCodec
	}

	headSize := GetHeadSize(pkType.headType)
	if headSize < 0 {
		return nil, kkerrors.ErrInvalidMsgHeadType
	}

	msgID := GetMsgID(v)
	if msgID == 0 {
		return nil, kkerrors.ErrMsgTypeNotRegistered
	}

	lfbCount := stream.LengthFieldByteCount()

	buf, err := codec.MarshalAppend(v, headSize+lfbCount)
	if err != nil {
		return nil, kkerrors.ErrEncodeFailed
	}

	bodyLen := len(buf.B) - headSize - lfbCount
	if bodyLen < 0 {
		return nil, kkerrors.ErrInvalidMsgHeadType
	}

	stream.writeBodySize(buf.B[:lfbCount], bodyLen)

	endian := GetByteOrder()
	switch pkType.headType {
	case HeadTypeMid:
		endian.PutUint32(buf.B[lfbCount:lfbCount+4], msgID)
	case HeadTypeMidSeq:
		seq := uint32(0)
		endian.PutUint32(buf.B[lfbCount:lfbCount+4], msgID)
		endian.PutUint32(buf.B[lfbCount+4:lfbCount+8], seq)
	default:
		kkbuffer.Put(buf)
		return nil, kkerrors.ErrInvalidMsgHeadType
	}

	return buf, nil
}

/*
*
解码包。

	@param data []byte 包数据[length,message]
	@param stream IStreamPacket 流包类型
	@return any 消息对象（object）
	@return MSGID 消息ID
	@return error 错误
*/
func DecodeStream(data []byte, stream IStreamPacket) (any, MSGID, error) {
	return DecodePacket(data[stream.LengthFieldByteCount():], stream.GetMessagePacket())
}
