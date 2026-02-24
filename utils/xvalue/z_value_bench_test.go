package xvalue_test

import (
	"testing"

	"github.com/vvisun/kkdg/utils/xvalue"
)

func BenchmarkNewValue(b *testing.B) {
	v := 42
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = xvalue.NewValue(v)
	}
}

func BenchmarkValue_Int_FromInt64(b *testing.B) {
	val := xvalue.NewValue(int64(42))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = val.Int()
	}
}

func BenchmarkValue_Int_FromString(b *testing.B) {
	val := xvalue.NewValue("12345")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = val.Int()
	}
}

func BenchmarkValue_Int64(b *testing.B) {
	val := xvalue.NewValue(int64(9876543210))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = val.Int64()
	}
}

func BenchmarkValue_Float64(b *testing.B) {
	val := xvalue.NewValue(3.14159)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = val.Float64()
	}
}

func BenchmarkValue_String(b *testing.B) {
	val := xvalue.NewValue("hello world")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = val.String()
	}
}

func BenchmarkValue_Bool(b *testing.B) {
	val := xvalue.NewValue(true)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = val.Bool()
	}
}

func BenchmarkValue_Value(b *testing.B) {
	val := xvalue.NewValue(42)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = val.Value()
	}
}

func BenchmarkValue_Kind(b *testing.B) {
	val := xvalue.NewValue(42)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = val.Kind()
	}
}

func BenchmarkValue_IsNumber(b *testing.B) {
	val := xvalue.NewValue(42)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = val.IsNumber()
	}
}

func BenchmarkValue_Ints(b *testing.B) {
	val := xvalue.NewValue([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = val.Ints()
	}
}

func BenchmarkValue_Strings(b *testing.B) {
	val := xvalue.NewValue([]string{"a", "b", "c", "d", "e"})
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = val.Strings()
	}
}

func BenchmarkValue_Slice(b *testing.B) {
	val := xvalue.NewValue([]int64{1, 2, 3, 4, 5})
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = val.Slice()
	}
}

func BenchmarkValue_Map(b *testing.B) {
	val := xvalue.NewValue(map[string]any{"id": 1, "name": "test"})
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = val.Map()
	}
}

func BenchmarkValue_Duration(b *testing.B) {
	val := xvalue.NewValue("1h30m")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = val.Duration()
	}
}

func BenchmarkValue_Scan_Int(b *testing.B) {
	val := xvalue.NewValue(42)
	var dst int
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = val.Scan(&dst)
	}
}

func BenchmarkValue_Scan_String(b *testing.B) {
	val := xvalue.NewValue("hello")
	var dst string
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = val.Scan(&dst)
	}
}
