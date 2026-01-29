package proto

import (
	"errors"

	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"google.golang.org/protobuf/proto"
)

const Name = "proto"

var DefaultCodec = &codec{}

type codec struct{}

// Name 编解码器名称
func (codec) Name() string {
	return Name
}

// Marshal 编码
func (codec) Marshal(v any) ([]byte, error) {
	msg, ok := v.(proto.Message)
	if !ok {
		return nil, errors.New("can't marshal a value that not implements proto.Buffer interface")
	}
	return proto.Marshal(msg)
}

// MarshalAppend 编码
// func (codec) MarshalAppend(v any, offset int) (*kkbuffer.ByteBuffer, error) {
// 	msg, ok := v.(proto.Message)
// 	if !ok {
// 		return nil, errors.New("can't marshal a value that not implements proto.Buffer interface")
// 	}
// 	size := proto.Size(msg) + offset + 64
// 	bb := kkbuffer.GetWithCapacity(size)
// 	bb.B = bb.B[:size]
// 	bytes, err := proto.MarshalOptions{}.MarshalAppend(bb.B[offset:], msg)
// 	if err != nil {
// 		return nil, err
// 	}
// 	realLen := len(bytes) + offset
// 	bb.B = bb.B[:realLen]
// 	return bb, nil
// }

// MarshalAppend 编码
func (codec) MarshalAppend(v any, offset int) (*kkbuffer.ByteBuffer, error) {
	msg, ok := v.(proto.Message)
	if !ok {
		return nil, errors.New("can't marshal a value that not implements proto.Buffer interface")
	}
	bytes, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	bb := kkbuffer.GetWithCapacity(len(bytes) + offset)
	bb.B = bb.B[:len(bytes)+offset]
	copy(bb.B[offset:], bytes)
	realLen := len(bytes) + offset
	bb.B = bb.B[:realLen]
	return bb, nil
}

// Unmarshal 解码
func (codec) Unmarshal(data []byte, v any) error {
	msg, ok := v.(proto.Message)
	if !ok {
		return errors.New("can't unmarshal to a value that not implements proto.Buffer")
	}
	return proto.Unmarshal(data, msg)
}

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
