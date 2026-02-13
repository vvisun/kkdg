package flatbuffer

import (
	"sync"
	"sync/atomic"
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
// 并发安全测试 (go test -race 可检测 data race)

func TestFlatBuffer_Concurrent_Marshal_Unmarshal_Int32(t *testing.T) {
	const n = 200
	var wg sync.WaitGroup
	errCount := atomic.Int32{}
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(id int32) {
			defer wg.Done()
			obj := &packableInt32{value: id}
			data, err := DefaultCodec.Marshal(obj)
			if err != nil {
				errCount.Add(1)
				return
			}
			var result fbbase.Int32
			if err := DefaultCodec.Unmarshal(data, &result); err != nil {
				errCount.Add(1)
				return
			}
			if result.Value() != id {
				errCount.Add(1)
			}
		}(int32(i))
	}
	wg.Wait()
	if errCount.Load() != 0 {
		t.Errorf("concurrent Marshal/Unmarshal Int32: %d errors", errCount.Load())
	}
}

func TestFlatBuffer_Concurrent_Marshal_Unmarshal_FrameStruct(t *testing.T) {
	const n = 200
	var wg sync.WaitGroup
	errCount := atomic.Int32{}
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(id uint64) {
			defer wg.Done()
			obj := &fbrpc.FrameStruct{T: 1, ID: id, M: "method", P: []byte("payload")}
			data, err := DefaultCodec.Marshal(obj)
			if err != nil {
				errCount.Add(1)
				return
			}
			var result fbrpc.FrameStruct
			if err := DefaultCodec.Unmarshal(data, &result); err != nil {
				errCount.Add(1)
				return
			}
			if result.ID != id || result.M != "method" || string(result.P) != "payload" {
				errCount.Add(1)
			}
		}(uint64(i))
	}
	wg.Wait()
	if errCount.Load() != 0 {
		t.Errorf("concurrent Marshal/Unmarshal FrameStruct: %d errors", errCount.Load())
	}
}

func TestFlatBuffer_Concurrent_MarshalAppend(t *testing.T) {
	const n = 200
	offset := 4
	var wg sync.WaitGroup
	errCount := atomic.Int32{}
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(id int32) {
			defer wg.Done()
			obj := &packableInt32{value: id}
			bb, err := DefaultCodec.MarshalAppend(obj, offset)
			if err != nil {
				errCount.Add(1)
				return
			}
			defer kkbuffer.Put(bb)
			var result fbbase.Int32
			if err := DefaultCodec.Unmarshal(bb.B[offset:], &result); err != nil {
				errCount.Add(1)
				return
			}
			if result.Value() != id {
				errCount.Add(1)
			}
		}(int32(i))
	}
	wg.Wait()
	if errCount.Load() != 0 {
		t.Errorf("concurrent MarshalAppend: %d errors", errCount.Load())
	}
}

func TestFlatBuffer_Concurrent_Mixed(t *testing.T) {
	const n = 100
	var wg sync.WaitGroup
	errCount := atomic.Int32{}
	wg.Add(n * 3)
	for i := 0; i < n; i++ {
		id := int32(i)
		// Marshal + Unmarshal (Int32)
		go func() {
			defer wg.Done()
			obj := &packableInt32{value: id}
			data, err := DefaultCodec.Marshal(obj)
			if err != nil {
				errCount.Add(1)
				return
			}
			var result fbbase.Int32
			if err := DefaultCodec.Unmarshal(data, &result); err != nil || result.Value() != id {
				errCount.Add(1)
			}
		}()
		// MarshalAppend
		go func() {
			defer wg.Done()
			obj := &fbrpc.FrameStruct{T: 2, ID: uint64(id), M: "m"}
			bb, err := DefaultCodec.MarshalAppend(obj, 4)
			if err != nil {
				errCount.Add(1)
				return
			}
			kkbuffer.Put(bb)
		}()
		// Marshal + Unmarshal (FrameStruct)
		go func() {
			defer wg.Done()
			obj := &fbrpc.FrameStruct{T: 3, ID: uint64(id + 1000), M: "mixed"}
			data, err := DefaultCodec.Marshal(obj)
			if err != nil {
				errCount.Add(1)
				return
			}
			var result fbrpc.FrameStruct
			if err := DefaultCodec.Unmarshal(data, &result); err != nil || result.ID != uint64(id+1000) {
				errCount.Add(1)
			}
		}()
	}
	wg.Wait()
	if errCount.Load() != 0 {
		t.Errorf("concurrent mixed: %d errors", errCount.Load())
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
