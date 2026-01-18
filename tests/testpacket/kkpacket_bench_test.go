package testpacket

import (
	"testing"

	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

func Benchmark_kkpacket_Encode(b *testing.B) {
	initTestEnv(nil)
	msg := &msgTest1{
		ID:   1,
		Data: "hello",
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		kkpacket.EncodePacket(msg, kkpacket.HeadTypeMid, kkcodec.CodecTypeJson)
	}
}

func Benchmark_kkpacket_EncodeEx(b *testing.B) {
	initTestEnv(nil)
	msg := &msgTest1{
		ID:   1,
		Data: "hello",
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		kkpacket.EncodePacketEx(msg, kkpacket.HeadTypeMid, kkcodec.CodecTypeJson)
	}
}

func Benchmark_kkpacket_Decode(b *testing.B) {
	initTestEnv(nil)
	msg := &msgTest1{
		ID:   1,
		Data: "hello",
	}
	packet, err := kkpacket.EncodePacket(msg, kkpacket.HeadTypeMid, kkcodec.CodecTypeJson)
	if err != nil {
		b.Fatalf("encode packet: %v", err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		kkpacket.DecodePacketEx[msgTest1](packet, kkpacket.HeadTypeMid, kkcodec.CodecTypeJson)
	}
}

func Benchmark_kkpacket_EncodeDecode(b *testing.B) {
	initTestEnv(nil)
	msg := &msgTest1{
		ID:   1,
		Data: "hello",
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		packet, err := kkpacket.EncodePacket(msg, kkpacket.HeadTypeMid, kkcodec.CodecTypeJson)
		if err != nil {
			b.Fatalf("encode packet: %v", err)
		}
		_, err = kkpacket.DecodePacketEx[msgTest1](packet, kkpacket.HeadTypeMid, kkcodec.CodecTypeJson)
		if err != nil {
			b.Fatalf("decode packet: %v", err)
		}
	}
}
