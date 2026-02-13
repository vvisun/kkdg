package flatbuffer

import (
	"sync"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/buffers/byteslice"
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

var (
	poolBuilder = sync.Pool{
		New: func() interface{} {
			bd := flatbuffers.NewBuilder(0)
			return bd
		},
	}
)

func GetBuilder() *flatbuffers.Builder {
	return poolBuilder.Get().(*flatbuffers.Builder)
}

func PutBuilder(builder *flatbuffers.Builder) {
	builder.Reset()
	poolBuilder.Put(builder)
}

// Marshal 编码
func (codec) Marshal(v any) ([]byte, error) {
	packable, ok := v.(FlatBufferPackable)
	if !ok {
		return nil, kkerrors.ErrCannotMarshalFlatBuffer
	}
	builder := GetBuilder()
	offset := packable.Pack(builder)
	builder.Finish(offset)
	bytes := builder.FinishedBytes()
	builder.Bytes = byteslice.Get(cap(bytes)) //替换掉原来的bytes，避免put后被重新使用污染返回的bytes
	PutBuilder(builder)
	return bytes, nil
}

// MarshalAppend 编码
func (codec) MarshalAppend(v any, offset int) (*kkbuffer.ByteBuffer, error) {
	packable, ok := v.(FlatBufferPackable)
	if !ok {
		return nil, kkerrors.ErrCannotMarshalFlatBuffer
	}
	builder := GetBuilder()
	root := packable.Pack(builder)
	builder.Finish(root)
	fbBytes := builder.FinishedBytes()
	realLen := len(fbBytes) + offset
	bb := kkbuffer.GetWithCapacity(realLen)
	bb.B = bb.B[:realLen]
	copy(bb.B[offset:], fbBytes)
	PutBuilder(builder)
	return bb, nil
}

// Unmarshal 解码，v 必须为 *Xxx（flatc 生成的 Table）或 *XxxStruct（fbs2struct 生成的 Unmarshaler）
func (codec) Unmarshal(data []byte, v any) error {
	if unmarshaler, ok := v.(FlatBufferUnmarshaler); ok {
		return unmarshaler.UnmarshalFlatBuffer(data)
	}
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
