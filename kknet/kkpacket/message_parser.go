package kkpacket

import (
	"encoding/binary"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kkcodec"
	"github.com/vvisun/kkdg/utils/kkpool"
)

// message = head + body

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

/*
*
解码包。
注意：外部需记得释放消息对象！！！否则消息对象得不到回收，性能反而更低！！！

	@param data []byte 包数据
	@param pkType *packer 包类型
	@return *T 消息对象
	@return error 错误
*/
func DecodePacket[T any](data []byte, pkType *PacketCodec) (*T, error) {
	headSize := GetHeadSize(pkType.headType)
	if headSize < 0 {
		return nil, kkerrors.ErrInvalidMsgHeadType
	}
	if len(data) < headSize {
		return nil, kkerrors.ErrDataTooShortToDecode
	}
	codec := kkcodec.GetCodec(pkType.codecType)
	if codec == nil {
		return nil, kkerrors.ErrInvalidCodec
	}

	endian := GetByteOrder()
	switch pkType.headType {
	case HeadTypeMid:
		head := ParseHeadMid(data[:headSize], endian)
		head.mid = endian.Uint32(data[:4])
		if head.mid == 0 {
			return nil, kkerrors.ErrInvalidMsgHeadType
		}
		if GetMsgType(head.mid) == nil {
			return nil, kkerrors.ErrMsgIDNotRegistered
		}
	case HeadTypeMidSeq:
		head := ParseHeadMidSeq(data[:headSize], endian)
		head.mid = endian.Uint32(data[:4])
		head.seq = endian.Uint32(data[4:8])
		if head.mid == 0 {
			return nil, kkerrors.ErrInvalidMsgHeadType
		}
		if GetMsgType(head.mid) == nil {
			return nil, kkerrors.ErrMsgIDNotRegistered
		}
	}

	body := data[headSize:]
	v := kkpool.GetFactory[T]().Get().(*T)
	err := codec.Unmarshal(body, v)
	if err != nil {
		kkpool.GetFactory[T]().Put(v)
		return nil, kkerrors.ErrDecodeFailed
	}
	return v, nil
}

/*
解码包。
注意：外部需记得释放消息对象！！！否则消息对象得不到回收，性能反而更低！！！

	@param data []byte 包数据
	@param pkType *packer 包类型
	@return any 消息对象
	@return error 错误
*/
func DecodePacketBytes(data []byte, pkType *PacketCodec) (any, error) {
	headSize := GetHeadSize(pkType.headType)
	if headSize < 0 || len(data) < headSize {
		return nil, kkerrors.ErrInvalidMsgHeadType
	}
	codec := kkcodec.GetCodec(pkType.codecType)
	if codec == nil {
		return nil, kkerrors.ErrInvalidCodec
	}

	endian := GetByteOrder()
	msgID := uint32(0)
	switch pkType.headType {
	case HeadTypeMid:
		head := ParseHeadMid(data[:headSize], endian)
		head.mid = endian.Uint32(data[:4])
		if head.mid == 0 {
			return nil, kkerrors.ErrInvalidMsgHeadType
		}
		if GetMsgType(head.mid) == nil {
			return nil, kkerrors.ErrMsgIDNotRegistered
		}
		msgID = head.mid
	case HeadTypeMidSeq:
		head := ParseHeadMidSeq(data[:headSize], endian)
		head.mid = endian.Uint32(data[:4])
		head.seq = endian.Uint32(data[4:8])
		if head.mid == 0 {
			return nil, kkerrors.ErrInvalidMsgHeadType
		}
		if GetMsgType(head.mid) == nil {
			return nil, kkerrors.ErrMsgIDNotRegistered
		}
		msgID = head.mid
	}

	msgType := GetMsgType(msgID)
	if msgType == nil {
		return nil, kkerrors.ErrMsgIDNotRegistered
	}

	body := data[headSize:]
	v := kkpool.GetFactoryByType(msgType).Get()
	err := codec.Unmarshal(body, v)
	if err != nil {
		kkpool.GetFactoryByType(msgType).Put(v)
		return nil, kkerrors.ErrDecodeFailed
	}
	return v, nil
}

/*
编码包

	@param v *T 消息类型
	@param pkType *packer 包类型
	@return []byte 包数据
	@return error 错误
*/
func EncodePacket[T any](v *T, pkType *PacketCodec) ([]byte, error) {
	codec := kkcodec.GetCodec(pkType.codecType)
	if codec == nil {
		return nil, kkerrors.ErrInvalidCodec
	}
	msgID := GetMsgID(v)
	if msgID == 0 {
		return nil, kkerrors.ErrMsgTypeNotRegistered
	}
	seq := uint32(0)
	headSize := GetHeadSize(pkType.headType)
	if headSize < 0 {
		return nil, kkerrors.ErrInvalidMsgHeadType
	}
	endian := GetByteOrder()
	head := make([]byte, headSize)
	switch pkType.headType {
	case HeadTypeMid:
		endian.PutUint32(head[:4], msgID)
	case HeadTypeMidSeq:
		endian.PutUint32(head[:4], msgID)
		endian.PutUint32(head[4:8], seq)
	default:
		return nil, kkerrors.ErrInvalidMsgHeadType
	}

	body, err := codec.Marshal(v)
	if err != nil {
		return nil, kkerrors.ErrEncodeFailed
	}
	return append(head, body...), nil
}

/*
编码包。
注意：外部需记得释放缓冲区！！！否则缓冲区得不到回收，性能反而更低！！！

	@param v *T 消息类型
	@param pkType *packer 包类型
	@return buffers.IBuffer 包数据
	@return error 错误
*/
func EncodePacketEx[T any](v *T, pkType *PacketCodec) (buffers.IBuffer, error) {
	codec := kkcodec.GetCodec(pkType.codecType)
	if codec == nil {
		return nil, kkerrors.ErrInvalidCodec
	}
	msgID := GetMsgID(v)
	if msgID == 0 {
		return nil, kkerrors.ErrMsgTypeNotRegistered
	}
	seq := uint32(0)
	headSize := GetHeadSize(pkType.headType)
	if headSize < 0 {
		return nil, kkerrors.ErrInvalidMsgHeadType
	}

	body, err := codec.Marshal(v)
	if err != nil {
		return nil, kkerrors.ErrEncodeFailed
	}

	bodyLen := len(body)

	buf := kkbuffer.GetWithCapacity(headSize + bodyLen)
	// Set the length to the total size we need
	buf.B = buf.B[:headSize+bodyLen]

	endian := GetByteOrder()
	switch pkType.headType {
	case HeadTypeMid:
		endian.PutUint32(buf.B[:4], msgID)
	case HeadTypeMidSeq:
		endian.PutUint32(buf.B[:4], msgID)
		endian.PutUint32(buf.B[4:8], seq)
	default:
		kkbuffer.Put(buf)
		return nil, kkerrors.ErrInvalidMsgHeadType
	}

	if bodyLen > 0 {
		copy(buf.B[headSize:], body)
	}

	return buf, nil
}
