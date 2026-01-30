package msgpack

import (
	"github.com/shamaton/msgpack/v2"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

const Name = "msgpack"

var DefaultCodec = &codec{}

type codec struct{}

// Name 编解码器名称
func (codec) Name() string {
	return Name
}

// Marshal 编码
func (codec) Marshal(v any) ([]byte, error) {
	return msgpack.Marshal(v)
}

// MarshalAppend 编码
func (codec) MarshalAppend(v any, offset int) (*kkbuffer.ByteBuffer, error) {
	bytes, err := msgpack.Marshal(v)
	if err != nil {
		return nil, err
	}
	realLen := len(bytes) + offset
	bb := kkbuffer.GetWithCapacity(realLen)
	bb.B = bb.B[:realLen]
	copy(bb.B[offset:], bytes)
	return bb, nil
}

// Unmarshal 解码
func (codec) Unmarshal(data []byte, v any) error {
	return msgpack.Unmarshal(data, v)
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
