package kkpacket

import (
	"encoding/binary"

	"github.com/vvisun/kkdg/kkerrors"
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

func (h *HeadMid) Marshal(data []byte, endian binary.ByteOrder) error {
	endian.PutUint32(data[:4], h.mid)
	return nil
}

func (h *HeadMid) Unmarshal(data []byte, endian binary.ByteOrder) error {
	h.mid = endian.Uint32(data[:4])
	return nil
}

type HeadMidSeq struct {
	mid uint32
	seq uint32
}

func (h *HeadMidSeq) Marshal(data []byte, endian binary.ByteOrder) error {
	endian.PutUint32(data[:4], h.mid)
	endian.PutUint32(data[4:8], h.seq)
	return nil
}

func (h *HeadMidSeq) Unmarshal(data []byte, endian binary.ByteOrder) error {
	h.mid = endian.Uint32(data[:4])
	h.seq = endian.Uint32(data[4:8])
	return nil
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
		head := HeadMid{}
		head.Unmarshal(data[:headSize], GetByteOrder())
		msgId = head.mid
	case HeadTypeMidSeq:
		head := HeadMidSeq{}
		head.Unmarshal(data[:headSize], GetByteOrder())
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
func DecodePacket(data []byte, pkType *PacketCodec, router *Router) (any, MSGID, error) {
	codec := kkcodec.GetCodec(pkType.codecType)
	if codec == nil {
		return nil, 0, kkerrors.ErrInvalidCodec
	}

	msgID, body, err := ParseMsgInfo(data, pkType)
	if err != nil {
		return nil, 0, err
	}

	msgType := router.GetMsgType(msgID)
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
func EncodePacket[T any](v *T, pkType *PacketCodec, router *Router) ([]byte, error) {
	buf, err := EncodePacketEx(v, pkType, router)
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
	@return *kkbuffer.ByteBuffer 包数据[message]
	@return error 错误
*/
func EncodePacketEx[T any](v *T, pkType *PacketCodec, router *Router) (*kkbuffer.ByteBuffer, error) {
	codec := kkcodec.GetCodec(pkType.codecType)
	if codec == nil {
		return nil, kkerrors.ErrInvalidCodec
	}

	headSize := GetHeadSize(pkType.headType)
	if headSize < 0 {
		return nil, kkerrors.ErrInvalidMsgHeadType
	}

	msgID := router.GetMsgID(v)
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
		head := HeadMid{mid: msgID}
		head.Marshal(buf.B[:headSize], endian)
	case HeadTypeMidSeq:
		head := HeadMidSeq{mid: msgID, seq: 0}
		head.Marshal(buf.B[:headSize], endian)
	default:
		kkbuffer.Put(buf)
		return nil, kkerrors.ErrInvalidMsgHeadType
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
func EncodeStream(v any, stream IStreamPacket, router *Router) (*kkbuffer.ByteBuffer, error) {
	pkType := stream.GetMessagePacket()
	codec := kkcodec.GetCodec(pkType.codecType)
	if codec == nil {
		return nil, kkerrors.ErrInvalidCodec
	}

	headSize := GetHeadSize(pkType.headType)
	if headSize < 0 {
		return nil, kkerrors.ErrInvalidMsgHeadType
	}

	msgID := router.GetMsgID(v)
	if msgID == 0 {
		return nil, kkerrors.ErrMsgTypeNotRegistered
	}

	lfbCount := stream.LengthFieldByteCount()

	buf, err := codec.MarshalAppend(v, lfbCount+headSize)
	if err != nil {
		return nil, kkerrors.ErrEncodeFailed
	}

	if len(buf.B)-lfbCount-headSize < 0 {
		return nil, kkerrors.ErrInvalidMsgHeadType
	}

	stream.writeBodySize(buf.B[:lfbCount], len(buf.B)-lfbCount)

	endian := GetByteOrder()
	switch pkType.headType {
	case HeadTypeMid:
		head := HeadMid{mid: msgID}
		head.Marshal(buf.B[lfbCount:lfbCount+headSize], endian)
	case HeadTypeMidSeq:
		head := HeadMidSeq{mid: msgID, seq: 0}
		head.Marshal(buf.B[lfbCount:lfbCount+headSize], endian)
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
func DecodeStream(data []byte, stream IStreamPacket, router *Router) (any, MSGID, error) {
	return DecodePacket(data[stream.LengthFieldByteCount():], stream.GetMessagePacket(), router)
}
