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

// FlatBufferUnmarshaler 可从 bytes 反序列化填充自身的结构体（由 fbs2struct 生成）
type FlatBufferUnmarshaler interface {
	UnmarshalFlatBuffer(data []byte) error
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

// MarshalStruct 泛型：将实现 FlatBufferPackable 的结构体编码为 bytes
func MarshalStruct[T FlatBufferPackable](v T) ([]byte, error) {
	return DefaultCodec.Marshal(v)
}

// UnmarshalStruct 泛型：将 bytes 解码到实现 FlatBufferUnmarshaler 的结构体
func UnmarshalStruct[T FlatBufferUnmarshaler](data []byte, v T) error {
	return v.UnmarshalFlatBuffer(data)
}

// DecodeToStruct 泛型便捷方法：解码并返回新分配的结构体（v 作为模板，需提供 NewT()）
// 若 T 支持指针接收的 UnmarshalFlatBuffer，可传入 &T{} 并通过返回值获取
func DecodeToStruct[T FlatBufferUnmarshaler](data []byte, newFunc func() T) (T, error) {
	v := newFunc()
	if err := v.UnmarshalFlatBuffer(data); err != nil {
		return v, err
	}
	return v, nil
}
