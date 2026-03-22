package ptotrans

import (
	"testing"

	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

func benchStructInfoAllKinds() (si structInfo, values []any) {
	si.AddField(dataTypeUint16, "u16")
	si.AddField(dataTypeUint32, "u32")
	si.AddField(dataTypeUint64, "u64")
	si.AddField(dataTypeUint8, "u8")
	si.AddField(dataTypeBool, "flag")
	si.AddField(dataTypeString, "s")
	si.AddField(dataTypeBytes, "b")
	si.AddField(dataTypeStringList, "sl")
	si.AddField(dataTypeBytesList, "bl")
	values = []any{
		uint16(0x1234),
		uint32(0x89abcdef),
		uint64(0x1122334455667788),
		uint8(0xfe),
		true,
		"hello 世界",
		[]byte{1, 2, 3, 0xff},
		[]string{"", "a", "bc"},
		[][]byte{nil, {}, {9, 9}},
	}
	return si, values
}

func benchStructInfoScalarsOnly() (si structInfo, values []any) {
	si.AddField(dataTypeUint16, "u16")
	si.AddField(dataTypeUint32, "u32")
	si.AddField(dataTypeUint64, "u64")
	si.AddField(dataTypeUint8, "u8")
	values = []any{uint16(1), uint32(2), uint64(3), uint8(4)}
	return si, values
}

// BenchmarkStructInfo_Marshal 含字符串与切片字段的典型负载。
func BenchmarkStructInfo_Marshal(b *testing.B) {
	si, values := benchStructInfoAllKinds()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bb, err := si.Marshal(values, 0)
		if err != nil {
			b.Fatal(err)
		}
		kkbuffer.Put(bb)
	}
}

// BenchmarkStructInfo_Marshal_ScalarsOnly 仅定长整数，便于对比变长字段开销。
func BenchmarkStructInfo_Marshal_ScalarsOnly(b *testing.B) {
	si, values := benchStructInfoScalarsOnly()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bb, err := si.Marshal(values, 0)
		if err != nil {
			b.Fatal(err)
		}
		kkbuffer.Put(bb)
	}
}

// BenchmarkStructInfo_Marshal_WithOffset 模拟包头预留（如前 16 字节给消息头）。
func BenchmarkStructInfo_Marshal_WithOffset(b *testing.B) {
	si, values := benchStructInfoAllKinds()
	const hdr = 16
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bb, err := si.Marshal(values, hdr)
		if err != nil {
			b.Fatal(err)
		}
		kkbuffer.Put(bb)
	}
}

func BenchmarkStructInfo_Unmarshal(b *testing.B) {
	si, values := benchStructInfoAllKinds()
	bb, err := si.Marshal(values, 0)
	if err != nil {
		b.Fatal(err)
	}
	encoded := append([]byte(nil), bb.B...)
	kkbuffer.Put(bb)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := si.Unmarshal(encoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStructInfo_Unmarshal_ScalarsOnly(b *testing.B) {
	si, values := benchStructInfoScalarsOnly()
	bb, err := si.Marshal(values, 0)
	if err != nil {
		b.Fatal(err)
	}
	encoded := append([]byte(nil), bb.B...)
	kkbuffer.Put(bb)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := si.Unmarshal(encoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStructInfo_Roundtrip Marshal + Unmarshal 连续执行。
func BenchmarkStructInfo_Roundtrip(b *testing.B) {
	si, values := benchStructInfoAllKinds()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bb, err := si.Marshal(values, 0)
		if err != nil {
			b.Fatal(err)
		}
		_, err = si.Unmarshal(bb.B)
		if err != nil {
			kkbuffer.Put(bb)
			b.Fatal(err)
		}
		kkbuffer.Put(bb)
	}
}

func BenchmarkStructInfo_MarshalParallel(b *testing.B) {
	si, values := benchStructInfoAllKinds()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bb, err := si.Marshal(values, 0)
			if err != nil {
				b.Fatal(err)
			}
			kkbuffer.Put(bb)
		}
	})
}

func BenchmarkStructInfo_UnmarshalParallel(b *testing.B) {
	si, values := benchStructInfoAllKinds()
	bb, err := si.Marshal(values, 0)
	if err != nil {
		b.Fatal(err)
	}
	encoded := append([]byte(nil), bb.B...)
	kkbuffer.Put(bb)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := si.Unmarshal(encoded)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
