package proto

import (
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"google.golang.org/protobuf/proto"
)

const Name = "proto"

type codec struct{}

// Name 编解码器名称
func (codec) Name() string {
	return Name
}

// Marshal 编码
func (codec) Marshal(v any) ([]byte, error) {
	msg, ok := v.(proto.Message)
	if !ok {
		return nil, kkerrors.ErrCannotUnmarshalToProtoMessage
	}
	return proto.Marshal(msg)
}

// MarshalAppend 编码
func (codec) MarshalAppend(v any, offset int) (*kkbuffer.ByteBuffer, error) {
	msg, ok := v.(proto.Message)
	if !ok {
		return nil, kkerrors.ErrCannotUnmarshalToProtoMessage
	}
	size := proto.Size(msg) + offset + 64 // 64 bytes more for sure enough capacity
	bb := kkbuffer.GetWithCapacity(size)
	bb.B = bb.B[:offset]
	bytes, err := proto.MarshalOptions{}.MarshalAppend(bb.B[:offset], msg)
	if err != nil {
		return nil, err
	}
	// bytes is the full result [reserved(offset) + marshaled], so length is offset + marshaled size
	bb.B = bb.B[:len(bytes)]
	return bb, nil
}

// Unmarshal 解码
func (codec) Unmarshal(data []byte, v any) error {
	msg, ok := v.(proto.Message)
	if !ok {
		return kkerrors.ErrCannotUnmarshalToProtoMessage
	}
	return proto.Unmarshal(data, msg)
}

var DefaultCodec = &codec{}

// Marshal 编码
func Marshal(v any) ([]byte, error) {
	return DefaultCodec.Marshal(v)
}

// Unmarshal 解码
func Unmarshal(data []byte, v any) error {
	return DefaultCodec.Unmarshal(data, v)
}

// MarshalAppend 编码
func MarshalAppend(v any, offset int) (*kkbuffer.ByteBuffer, error) {
	return DefaultCodec.MarshalAppend(v, offset)
}
