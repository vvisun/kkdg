package testpacket

import (
	"fmt"
	"testing"

	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

func BenchmarkLengthFieldPacker_Pack(b *testing.B) {
	sizes := []int{0, 64, 256, 1024, 4096, 16384}
	packer := kkpacket.NewLengthFieldPacker(65536, nil)

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%d", size), func(b *testing.B) {
			data := make([]byte, size)
			for i := range data {
				data[i] = byte(i % 256)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				buf, err := packer.Pack(data)
				if err != nil {
					b.Fatalf("Pack returned error: %v", err)
				}
				// Put buffer back to pool to simulate real usage
				kkbuffer.Put(buf)
			}
		})
	}
}

func BenchmarkLengthFieldPacker_Pack_Small(b *testing.B) {
	packer := kkpacket.NewLengthFieldPacker(1024, nil)
	data := []byte("hello")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		buf, err := packer.Pack(data)
		if err != nil {
			b.Fatalf("Pack returned error: %v", err)
		}
		kkbuffer.Put(buf)
	}
}

func BenchmarkLengthFieldPacker_Pack_Medium(b *testing.B) {
	packer := kkpacket.NewLengthFieldPacker(65536, nil)
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		buf, err := packer.Pack(data)
		if err != nil {
			b.Fatalf("Pack returned error: %v", err)
		}
		kkbuffer.Put(buf)
	}
}

func BenchmarkLengthFieldPacker_Pack_Large(b *testing.B) {
	packer := kkpacket.NewLengthFieldPacker(1048576, nil)
	data := make([]byte, 65536)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		buf, err := packer.Pack(data)
		if err != nil {
			b.Fatalf("Pack returned error: %v", err)
		}
		kkbuffer.Put(buf)
	}
}

func BenchmarkLengthFieldPacker_Pack_Empty(b *testing.B) {
	packer := kkpacket.NewLengthFieldPacker(1024, nil)
	data := []byte{}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		buf, err := packer.Pack(data)
		if err != nil {
			b.Fatalf("Pack returned error: %v", err)
		}
		kkbuffer.Put(buf)
	}
}

func BenchmarkLengthFieldPacker_Pack_WithoutPut(b *testing.B) {
	packer := kkpacket.NewLengthFieldPacker(65536, nil)
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		buf, err := packer.Pack(data)
		if err != nil {
			b.Fatalf("Pack returned error: %v", err)
		}
		_ = buf // Use buffer but don't put it back (simulates one-time use)
	}
}

func BenchmarkLengthFieldPacker_Pack_MaxSize(b *testing.B) {
	maxSize := 1024
	packer := kkpacket.NewLengthFieldPacker(maxSize, nil)
	data := make([]byte, maxSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		buf, err := packer.Pack(data)
		if err != nil {
			b.Fatalf("Pack returned error: %v", err)
		}
		kkbuffer.Put(buf)
	}
}

func BenchmarkLengthFieldPacker_Pack_VariousSizes(b *testing.B) {
	packer := kkpacket.NewLengthFieldPacker(65536, nil)
	testSizes := []int{1, 16, 128, 512, 2048, 8192}

	for _, size := range testSizes {
		b.Run(fmt.Sprintf("Size_%d", size), func(b *testing.B) {
			data := make([]byte, size)
			for i := range data {
				data[i] = byte(i % 256)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				buf, err := packer.Pack(data)
				if err != nil {
					b.Fatalf("Pack returned error: %v", err)
				}
				kkbuffer.Put(buf)
			}
		})
	}
}
