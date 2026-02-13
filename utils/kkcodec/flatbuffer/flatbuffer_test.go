package flatbuffer

import (
	"testing"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/vvisun/kkdg/proto/pbbase/fbbase"
	"github.com/vvisun/kkdg/proto/pbrpc/fbrpc"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// packableInt32 实现 FlatBufferPackable，用于测试 Marshal
type packableInt32 struct{ value int32 }

func (p *packableInt32) Pack(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	fbbase.Int32Start(builder)
	fbbase.Int32AddValue(builder, p.value)
	return fbbase.Int32End(builder)
}

func TestFlatBuffer_Marshal_Unmarshal(t *testing.T) {
	// Marshal: *packableInt32 实现 FlatBufferPackable
	obj := &packableInt32{value: 42}
	data, err := DefaultCodec.Marshal(obj)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("Marshal: empty output")
	}

	// Unmarshal: *fbbase.Int32 实现 FlatBufferTable (Init)
	var result fbbase.Int32
	err = DefaultCodec.Unmarshal(data, &result)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if result.Value() != 42 {
		t.Errorf("Unmarshal: got %d, want 42", result.Value())
	}
}

func TestFlatBuffer_MarshalAppend(t *testing.T) {
	obj := &packableInt32{value: 99}
	offset := 4
	bb, err := DefaultCodec.MarshalAppend(obj, offset)
	if err != nil {
		t.Fatalf("MarshalAppend: %v", err)
	}
	defer kkbuffer.Put(bb)
	if len(bb.B) < offset {
		t.Fatalf("MarshalAppend: len=%d, want >= %d", len(bb.B), offset)
	}
	// 解码时从 offset 开始是完整的 flatbuffer 数据
	var result fbbase.Int32
	err = DefaultCodec.Unmarshal(bb.B[offset:], &result)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if result.Value() != 99 {
		t.Errorf("Unmarshal: got %d, want 99", result.Value())
	}
}

func TestFlatBuffer_Marshal_InvalidType(t *testing.T) {
	_, err := DefaultCodec.Marshal(123)
	if err == nil {
		t.Fatal("Marshal: expected error for invalid type")
	}
}

func TestFlatBuffer_Unmarshal_InvalidType(t *testing.T) {
	data := []byte{0, 0, 0, 0} // 最小有效 flatbuffer
	err := DefaultCodec.Unmarshal(data, 123)
	if err == nil {
		t.Fatal("Unmarshal: expected error for invalid type")
	}
}

//----------------------------------------------------------------
// 性能测试 (Table 类型)

var benchInt32Obj = &packableInt32{value: 42}
var benchInt32Data []byte

func init() {
	benchInt32Data, _ = DefaultCodec.Marshal(benchInt32Obj)
}

func Benchmark_Marshal_Int32Table(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := DefaultCodec.Marshal(benchInt32Obj)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Unmarshal_Int32Table(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var result fbbase.Int32
		err := DefaultCodec.Unmarshal(benchInt32Data, &result)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_MarshalAppend_FrameStruct(b *testing.B) {
	obj := &fbrpc.FrameStruct{T: 1, ID: 100, M: "test", P: []byte("payload")}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		bb, err := DefaultCodec.MarshalAppend(obj, 4)
		if err != nil {
			b.Fatal(err)
		}
		kkbuffer.Put(bb)
	}
}
