package xrand_test

import (
	"testing"

	"github.com/vvisun/kkdg/utils/xconv"
	"github.com/vvisun/kkdg/utils/xrand"
)

var (
	benchStr   string
	benchInt   int
	benchF32   float32
	benchBool  bool
	benchIndex int
)

func BenchmarkLetters16(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchStr = xrand.Letters(16)
	}
}

func BenchmarkDigits16(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchStr = xrand.Digits(16, true)
	}
}

func BenchmarkSymbols16(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchStr = xrand.Symbols(16)
	}
}

func BenchmarkIntRange(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchInt = xrand.Int(1, 1000000)
	}
}

func BenchmarkFloat32Range(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchF32 = xrand.Float32(-50, 5000)
	}
}

func BenchmarkLucky(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchBool = xrand.Lucky(50.201222)
	}
}

func BenchmarkWeight3(b *testing.B) {
	b.ReportAllocs()
	weights := []any{50, 20.3, 29.7}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchIndex = xrand.Weight(func(v any) float64 {
			return xconv.Float64(v)
		}, weights...)
	}
}

func BenchmarkParallelIntRange(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			benchInt = xrand.Int(1, 1000000)
		}
	})
}

func BenchmarkParallelFloat32Range(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			benchF32 = xrand.Float32(-50, 5000)
		}
	})
}

func BenchmarkParallelLucky(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			benchBool = xrand.Lucky(50.201222)
		}
	})
}

func BenchmarkParallelWeight3(b *testing.B) {
	b.ReportAllocs()
	weights := []any{50, 20.3, 29.7}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			benchIndex = xrand.Weight(func(v any) float64 {
				return xconv.Float64(v)
			}, weights...)
		}
	})
}

func BenchmarkParallelLetters16(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			benchStr = xrand.Letters(16)
		}
	})
}
