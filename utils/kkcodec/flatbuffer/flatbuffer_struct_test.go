package flatbuffer

import (
	"testing"

	"github.com/vvisun/kkdg/proto/pbrpc/fbrpc"
)

func TestFlatBuffer_MarshalStruct_UnmarshalStruct(t *testing.T) {
	obj := &fbrpc.FrameStruct{
		T:    1,
		ID:   100,
		DL:   0,
		M:    "Hello",
		P:    []byte("payload"),
		Code: 0,
		Err:  "",
	}

	data, err := MarshalStruct(obj)
	if err != nil {
		t.Fatalf("MarshalStruct: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("MarshalStruct: empty output")
	}

	var result fbrpc.FrameStruct
	err = UnmarshalStruct(data, &result)
	if err != nil {
		t.Fatalf("UnmarshalStruct: %v", err)
	}
	if result.T != obj.T || result.ID != obj.ID || result.M != obj.M || string(result.P) != string(obj.P) {
		t.Errorf("UnmarshalStruct: got T=%d ID=%d M=%q P=%q, want T=%d ID=%d M=%q P=%q",
			result.T, result.ID, result.M, string(result.P),
			obj.T, obj.ID, obj.M, string(obj.P))
	}
}

func TestFlatBuffer_DecodeToStruct(t *testing.T) {
	obj := &fbrpc.FrameStruct{T: 42, M: "test"}
	data, _ := MarshalStruct(obj)

	result, err := DecodeToStruct(data, func() *fbrpc.FrameStruct { return &fbrpc.FrameStruct{} })
	if err != nil {
		t.Fatalf("DecodeToStruct: %v", err)
	}
	if result.T != 42 || result.M != "test" {
		t.Errorf("DecodeToStruct: got T=%d M=%q", result.T, result.M)
	}
}

//----------------------------------------------------------------
// 性能测试

var benchFrameStruct = &fbrpc.FrameStruct{
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
	benchFrameStructData, _ = MarshalStruct(benchFrameStruct)
}

func Benchmark_Marshal_FrameStruct(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := MarshalStruct(benchFrameStruct)
		if err != nil {
			b.Fatalf("MarshalStruct: %v", err)
		}
	}
}

func Benchmark_Unmarshal_FrameStruct(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var result fbrpc.FrameStruct
		err := UnmarshalStruct(benchFrameStructData, &result)
		if err != nil {
			b.Fatalf("UnmarshalStruct: %v", err)
		}
	}
}

func Benchmark_Marshal_Unmarshal_FrameStruct(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		data, err := MarshalStruct(benchFrameStruct)
		if err != nil {
			b.Fatalf("MarshalStruct: %v", err)
		}
		var result fbrpc.FrameStruct
		err = UnmarshalStruct(data, &result)
		if err != nil {
			b.Fatalf("UnmarshalStruct: %v", err)
		}
	}
}
