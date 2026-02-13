package flatbuffer

import (
	"github.com/google/flatbuffers/go"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

const Name = "flatbuffer"

// FlatBufferPackable 可被 FlatBuffers 序列化的类型，通常为 *XxxT（由 flatc 生成）
type FlatBufferPackable interface {
	Pack(builder *flatbuffers.Builder) flatbuffers.UOffsetT
}

// FlatBufferTable 可被 FlatBuffers 反序列化的类型，通常为 *Xxx（由 flatc 生成）
type FlatBufferTable interface {
	Init(buf []byte, i flatbuffers.UOffsetT)
}

type codec struct{}

// Name 编解码器名称
func (codec) Name() string {
	return Name
}

// Marshal 编码
func (codec) Marshal(v any) ([]byte, error) {
	packable, ok := v.(FlatBufferPackable)
	if !ok {
		return nil, kkerrors.ErrCannotMarshalFlatBuffer
	}
	builder := flatbuffers.NewBuilder(0)
	offset := packable.Pack(builder)
	builder.Finish(offset)
	return builder.FinishedBytes(), nil
}

// MarshalAppend 编码
func (codec) MarshalAppend(v any, offset int) (*kkbuffer.ByteBuffer, error) {
	packable, ok := v.(FlatBufferPackable)
	if !ok {
		return nil, kkerrors.ErrCannotMarshalFlatBuffer
	}
	builder := flatbuffers.NewBuilder(0)
	root := packable.Pack(builder)
	builder.Finish(root)
	fbBytes := builder.FinishedBytes()
	realLen := len(fbBytes) + offset
	bb := kkbuffer.GetWithCapacity(realLen)
	bb.B = bb.B[:realLen]
	copy(bb.B[offset:], fbBytes)
	return bb, nil
}

// Unmarshal 解码，v 必须为 *Xxx 类型（flatc 生成的 Table 类型）
func (codec) Unmarshal(data []byte, v any) error {
	table, ok := v.(FlatBufferTable)
	if !ok {
		return kkerrors.ErrCannotUnmarshalFlatBuffer
	}
	if len(data) < flatbuffers.SizeUOffsetT {
		return kkerrors.ErrDataTooShortToUnmarshal
	}
	n := flatbuffers.GetUOffsetT(data)
	table.Init(data, n)
	return nil
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
