package kkpacket

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

func benchMsg(size int) []byte {
	msg := make([]byte, size)
	for i := 0; i < size; i++ {
		msg[i] = byte(i % 256)
	}
	return msg
}

func BenchmarkLengthFieldStreamPacket_Pack(b *testing.B) {
	for _, size := range []int{0, 16, 256, 1024} {
		b.Run(byteSizeLabel(size), func(b *testing.B) {
			p := NewLengthFieldStreamPacket(4, 4*1024)
			msg := benchMsg(size)

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				bb, err := p.Pack(msg)
				if err != nil {
					b.Fatal(err)
				}
				kkbuffer.Put(bb)
			}
		})
	}
}

func BenchmarkLengthFieldStreamPacket_Unpack(b *testing.B) {
	for _, size := range []int{0, 16, 256, 1024} {
		b.Run(byteSizeLabel(size), func(b *testing.B) {
			p := NewLengthFieldStreamPacket(4, 4*1024)
			bb, _ := p.Pack(benchMsg(size))
			packet := bb.B
			kkbuffer.Put(bb)

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, err := p.Unpack(packet)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkLengthFieldStreamPacket_PackUnpack(b *testing.B) {
	for _, size := range []int{0, 16, 256, 1024, 4 * 1024} {
		b.Run(byteSizeLabel(size), func(b *testing.B) {
			p := NewLengthFieldStreamPacket(4, 4*1024+4)
			msg := benchMsg(size)

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				bb, err := p.Pack(msg)
				if err != nil {
					b.Fatal(err)
				}
				_, err = p.Unpack(bb.B)
				if err != nil {
					b.Fatal(err)
				}
				kkbuffer.Put(bb)
			}
		})
	}
}

func BenchmarkLengthFieldStreamPacket_ReadMessageSize(b *testing.B) {
	p := NewLengthFieldStreamPacket(4, 4*1024)
	bb, _ := p.Pack([]byte("bench"))
	header := bb.B[:4]
	kkbuffer.Put(bb)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := p.ReadMessageSize(header)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLengthFieldStreamPacket_WriteMessageSize(b *testing.B) {
	p := NewLengthFieldStreamPacket(4, 4*1024)
	buf := make([]byte, 4)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		p.WriteMessageSize(buf, 1024)
	}
}

func BenchmarkLengthFieldStreamPacket_CheckPacket(b *testing.B) {
	p := NewLengthFieldStreamPacket(4, 4*1024)
	bb, _ := p.Pack(benchMsg(64))
	packet := bb.B
	kkbuffer.Put(bb)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := p.CheckPacket(packet)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLengthFieldStreamPacket_Split(b *testing.B) {
	for _, n := range []int{1, 10, 100} {
		b.Run(packetCountLabel(n), func(b *testing.B) {
			p := NewLengthFieldStreamPacket(4, 4*1024)
			msg := benchMsg(64)
			var concat []byte
			for i := 0; i < n; i++ {
				bb, _ := p.Pack(msg)
				concat = append(concat, bb.B...)
				kkbuffer.Put(bb)
			}
			recvs := make([][]byte, 0, n)

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _, err := p.Split(concat, recvs)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkLengthFieldStreamPacket_SplitSR(b *testing.B) {
	p := NewLengthFieldStreamPacket(4, 4*1024)
	bb, _ := p.Pack(benchMsg(64))
	packetData := make([]byte, len(bb.B))
	copy(packetData, bb.B)
	kkbuffer.Put(bb)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r := newBenchStreamReader(packetData)
		_, _, err := p.SplitSR(r)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLengthFieldStreamPacket_LFB2_vs_LFB4(b *testing.B) {
	msg := benchMsg(256)
	for _, lfb := range []int{2, 4} {
		b.Run(lfbLabel(lfb), func(b *testing.B) {
			p := NewLengthFieldStreamPacket(lfb, 4*1024)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				bb, err := p.Pack(msg)
				if err != nil {
					b.Fatal(err)
				}
				_, err = p.Unpack(bb.B)
				if err != nil {
					b.Fatal(err)
				}
				kkbuffer.Put(bb)
			}
		})
	}
}

func byteSizeLabel(n int) string {
	if n < 1024 {
		return fmt.Sprintf("%dB", n)
	}
	return fmt.Sprintf("%dKB", n/1024)
}

func packetCountLabel(n int) string {
	return fmt.Sprintf("%dpkts", n)
}

func lfbLabel(lfb int) string {
	return fmt.Sprintf("LFB%d", lfb)
}

// benchStreamReader implements IStreamReader for SplitSR benchmark.
type benchStreamReader struct {
	buf bytes.Buffer
}

func newBenchStreamReader(data []byte) *benchStreamReader {
	r := &benchStreamReader{}
	r.buf.Write(data)
	return r
}

func (r *benchStreamReader) InboundBuffered() int {
	return r.buf.Len()
}

func (r *benchStreamReader) Peek(n int) ([]byte, error) {
	b := r.buf.Bytes()
	if len(b) < n {
		return nil, io.ErrShortBuffer
	}
	return b[:n], nil
}

func (r *benchStreamReader) Discard(n int) (int, error) {
	_, err := r.buf.Read(make([]byte, n))
	return n, err
}

func (r *benchStreamReader) Next(n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := io.ReadFull(&r.buf, b)
	return b, err
}
