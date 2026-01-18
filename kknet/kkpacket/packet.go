package kkpacket

import (
	"encoding/binary"
	"reflect"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

// message = head + body

const (
	HeadTypeMid    = 0
	HeadTypeMidSeq = 1
)

func GetHeadSize(headType int) int {
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
	Mid uint32
}

type HeadMidSeq struct {
	Mid uint32
	Seq uint32
}

func ParseHeadMid(data []byte) HeadMid {
	return HeadMid{
		Mid: binary.BigEndian.Uint32(data[:4]),
	}
}

func ParseHeadMidSeq(data []byte) HeadMidSeq {
	return HeadMidSeq{
		Mid: binary.BigEndian.Uint32(data[:4]),
		Seq: binary.BigEndian.Uint32(data[4:8]),
	}
}

/*
*
*
解码包

	@param data []byte 包数据
	@param headType int 头类型
	@param codecType int 编码器类型
	@return any 消息对象
	@return error 错误
*/
func DecodePacket(data []byte, headType int, codecType int) (interface{}, error) {
	headSize := GetHeadSize(headType)
	if headSize < 0 || len(data) < headSize {
		return nil, kkerrors.ErrInvalidPacket
	}
	codec := kkcodec.GetCodec(codecType)
	if codec == nil {
		return nil, kkerrors.ErrInvalidCodec
	}

	head := ParseHeadMid(data[:headSize])
	if head.Mid == 0 {
		return nil, kkerrors.ErrInvalidPacket
	}

	msgType := GetMsgType(head.Mid)
	if msgType == nil {
		return nil, kkerrors.ErrMsgIDNotRegistered
	}

	body := data[headSize:]
	v := reflect.New(msgType.Elem()).Interface()
	err := codec.Unmarshal(body, v)
	if err != nil {
		return nil, kkerrors.ErrDecodeFailed
	}
	return v, nil
}

func DecodePacketEx[T any](data []byte, headType int, codecType int) (*T, error) {
	headSize := GetHeadSize(headType)
	if headSize < 0 || len(data) < headSize {
		return nil, kkerrors.ErrInvalidPacket
	}
	codec := kkcodec.GetCodec(codecType)
	if codec == nil {
		return nil, kkerrors.ErrInvalidCodec
	}

	head := ParseHeadMid(data[:headSize])
	if head.Mid == 0 {
		return nil, kkerrors.ErrInvalidPacket
	}

	msgType := GetMsgType(head.Mid)
	if msgType == nil {
		return nil, kkerrors.ErrMsgIDNotRegistered
	}

	body := data[headSize:]
	var v T
	err := codec.Unmarshal(body, &v)
	if err != nil {
		return nil, kkerrors.ErrDecodeFailed
	}
	return &v, nil
}

/*
*
*
编码包

	@param v *T 消息类型
	@param headType int 头类型
	@param codecType int 编解码器类型
	@return []byte 包数据
	@return error 错误
*/
func EncodePacket[T any](v *T, headType int, codecType int) ([]byte, error) {
	codec := kkcodec.GetCodec(codecType)
	if codec == nil {
		return nil, kkerrors.ErrInvalidCodec
	}
	msgID := GetMsgID(v)
	if msgID == 0 {
		return nil, kkerrors.ErrMsgTypeNotRegistered
	}
	seq := uint32(0)
	headSize := GetHeadSize(headType)
	if headSize < 0 {
		return nil, kkerrors.ErrInvalidHeadType
	}
	head := make([]byte, headSize)
	switch headType {
	case HeadTypeMid:
		binary.BigEndian.PutUint32(head[:4], msgID)
	case HeadTypeMidSeq:
		binary.BigEndian.PutUint32(head[:4], msgID)
		binary.BigEndian.PutUint32(head[4:8], seq)
	default:
		return nil, kkerrors.ErrInvalidHeadType
	}

	body, err := codec.Marshal(v)
	if err != nil {
		return nil, kkerrors.ErrEncodeFailed
	}
	return append(head, body...), nil
}

/*
*
*
编码包

	@param v *T 消息类型
	@param headType int 头类型
	@param codecType int 编解码器类型
	@return []byte 包数据
	@return error 错误
*/
func EncodePacketEx[T any](v *T, headType int, codecType int) (buffers.IBuffer, error) {
	codec := kkcodec.GetCodec(codecType)
	if codec == nil {
		return nil, kkerrors.ErrInvalidCodec
	}
	msgID := GetMsgID(v)
	if msgID == 0 {
		return nil, kkerrors.ErrMsgTypeNotRegistered
	}
	seq := uint32(0)
	headSize := GetHeadSize(headType)
	if headSize < 0 {
		return nil, kkerrors.ErrInvalidHeadType
	}

	body, err := codec.Marshal(v)
	if err != nil {
		return nil, kkerrors.ErrEncodeFailed
	}

	bodyLen := len(body)

	buf := kkbuffer.GetWithCapacity(headSize + bodyLen)
	// Set the length to the total size we need
	buf.B = buf.B[:headSize+bodyLen]

	switch headType {
	case HeadTypeMid:
		binary.BigEndian.PutUint32(buf.B[:4], msgID)
	case HeadTypeMidSeq:
		binary.BigEndian.PutUint32(buf.B[:4], msgID)
		binary.BigEndian.PutUint32(buf.B[4:8], seq)
	default:
		return nil, kkerrors.ErrInvalidHeadType
	}

	if bodyLen > 0 {
		copy(buf.B[headSize:], body)
	}

	return buf, nil
}
