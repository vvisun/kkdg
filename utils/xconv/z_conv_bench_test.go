package xconv_test

import (
	"testing"
	"time"

	"github.com/vvisun/kkdg/utils/xconv"
)

func BenchmarkInt_FromInt64(b *testing.B) {
	v := int64(42)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = xconv.Int(v)
	}
}

func BenchmarkInt_FromString(b *testing.B) {
	v := "12345"
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = xconv.Int(v)
	}
}

func BenchmarkInt64_FromInt64(b *testing.B) {
	v := int64(9876543210)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = xconv.Int64(v)
	}
}

func BenchmarkInt64_FromString(b *testing.B) {
	v := "9876543210"
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = xconv.Int64(v)
	}
}

func BenchmarkString_FromInt(b *testing.B) {
	v := 42
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = xconv.String(v)
	}
}

func BenchmarkString_FromFloat64(b *testing.B) {
	v := 3.14159
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = xconv.String(v)
	}
}

func BenchmarkBool_FromInt(b *testing.B) {
	v := 1
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = xconv.Bool(v)
	}
}

func BenchmarkBytes_FromString(b *testing.B) {
	v := "hello world"
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = xconv.Bytes(v)
	}
}

func BenchmarkBytes_FromInt(b *testing.B) {
	v := 255
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = xconv.Bytes(v)
	}
}

func BenchmarkFloat64_FromString(b *testing.B) {
	v := "3.14159"
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = xconv.Float64(v)
	}
}

func BenchmarkDuration_FromString(b *testing.B) {
	v := "1h30m45s"
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = xconv.Duration(v)
	}
}

func BenchmarkDuration_FromInt64(b *testing.B) {
	v := int64(time.Second)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = xconv.Duration(v)
	}
}

func BenchmarkB_FromString(b *testing.B) {
	v := "1G"
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = xconv.B(v)
	}
}

func BenchmarkB_FromInt(b *testing.B) {
	v := 1024
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = xconv.B(v)
	}
}

func BenchmarkJson_FromMap(b *testing.B) {
	v := map[string]any{"id": 1, "name": "test", "score": 99.5}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = xconv.Json(v)
	}
}

func BenchmarkInts_FromInt64Slice(b *testing.B) {
	v := make([]int64, 100)
	for i := range v {
		v[i] = int64(i)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = xconv.Ints(v)
	}
}

func BenchmarkStrings_FromInt64Slice(b *testing.B) {
	v := make([]int64, 100)
	for i := range v {
		v[i] = int64(i)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = xconv.Strings(v)
	}
}

func BenchmarkAnys_FromInt64Slice(b *testing.B) {
	v := make([]int64, 100)
	for i := range v {
		v[i] = int64(i)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = xconv.Anys(v)
	}
}
