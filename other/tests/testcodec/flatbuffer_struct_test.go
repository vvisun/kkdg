package testcodec

import (
	"testing"

	"github.com/vvisun/kkdg/proto/pbrpc/fbtrpc"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

var usingCodec = kkcodec.GetCodec(kkcodec.CodecTypeFlatBuffer)

func TestFlatBuffer_MarshalStruct_UnmarshalStruct(t *testing.T) {
	obj := &fbtrpc.Frame{
		T:    1,
		ID:   100,
		DL:   0,
		M:    "Hello",
		P:    []byte("payload"),
		Code: 0,
		Err:  "",
	}

	data, err := usingCodec.Marshal(obj)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("Marshal: empty output")
	}

	var result fbtrpc.Frame
	err = usingCodec.Unmarshal(data, &result)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if result.T != obj.T || result.ID != obj.ID || result.M != obj.M || string(result.P) != string(obj.P) {
		t.Errorf("Unmarshal: got T=%d ID=%d M=%q P=%q, want T=%d ID=%d M=%q P=%q",
			result.T, result.ID, result.M, string(result.P),
			obj.T, obj.ID, obj.M, string(obj.P))
	}
}

func TestFlatBuffer_UnmarshalToNewStruct(t *testing.T) {
	obj := &fbtrpc.Frame{T: 42, M: "test"}
	data, _ := usingCodec.Marshal(obj)

	result := &fbtrpc.Frame{}
	err := usingCodec.Unmarshal(data, result)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if result.T != 42 || result.M != "test" {
		t.Errorf("Unmarshal: got T=%d M=%q", result.T, result.M)
	}
}

//----------------------------------------------------------------
// 性能测试

var benchFrameStruct = &fbtrpc.Frame{
	T:    1,
	ID:   100,
	DL:   0,
	M:    "test_method",
	P:    []byte("payload_data"),
	Code: 0,
	Err:  "",
}

var benchFrameStructData []byte

func init() {
	benchFrameStructData, _ = usingCodec.Marshal(benchFrameStruct)
}

func Benchmark_Marshal_FrameStruct(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := usingCodec.Marshal(benchFrameStruct)
		if err != nil {
			b.Fatalf("Marshal: %v", err)
		}
	}
}

func Benchmark_Unmarshal_FrameStruct(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var result fbtrpc.Frame
		err := usingCodec.Unmarshal(benchFrameStructData, &result)
		if err != nil {
			b.Fatalf("Unmarshal: %v", err)
		}
	}
}

func Benchmark_Marshal_Unmarshal_FrameStruct(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		data, err := usingCodec.Marshal(benchFrameStruct)
		if err != nil {
			b.Fatalf("Marshal: %v", err)
		}
		var result fbtrpc.Frame
		err = usingCodec.Unmarshal(data, &result)
		if err != nil {
			b.Fatalf("Unmarshal: %v", err)
		}
	}
}
